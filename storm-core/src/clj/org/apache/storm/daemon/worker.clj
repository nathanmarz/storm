;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns org.apache.storm.daemon.worker
  (:use [org.apache.storm.daemon common])
  (:use [org.apache.storm config log util converter local-state-converter])
  (:require [clj-time.core :as time])
  (:require [clj-time.coerce :as coerce])
  (:require [org.apache.storm.daemon [executor :as executor]])

  (:require [clojure.set :as set])
  (:import [java.io File])
  (:import [java.util.concurrent Executors]
           [org.apache.storm.hooks IWorkerHook BaseWorkerHook]
           [uk.org.lidalia.sysoutslf4j.context SysOutOverSLF4J])
  (:import [org.apache.storm.utils Utils ConfigUtils TransferDrainer ThriftTopologyUtils WorkerBackpressureThread DisruptorQueue Time WorkerBackpressureCallback DisruptorBackpressureCallback])
  (:import [java.util ArrayList HashMap]
           [java.util.concurrent.locks ReentrantReadWriteLock])
  (:import [org.apache.commons.io FileUtils])
  (:import [org.apache.storm.grouping LoadMapping])
  (:import [org.apache.storm.messaging TransportFactory])
  (:import [org.apache.storm.messaging TaskMessage IContext IConnection ConnectionWithStatus ConnectionWithStatus$Status DeserializingConnectionCallback])
  (:import [org.apache.storm.daemon Shutdownable])
  (:import [org.apache.storm.serialization KryoTupleSerializer])
  (:import [org.apache.storm.generated StormTopology LSWorkerHeartbeat])
  (:import [org.apache.storm.tuple AddressedTuple Fields])
  (:import [org.apache.storm.task WorkerTopologyContext])
  (:import [org.apache.storm Constants])
  (:import [org.apache.storm.security.auth AuthUtils])
  (:import [org.apache.storm.cluster ClusterStateContext DaemonType ZKStateStorage StormClusterStateImpl ClusterUtils IStateStorage])
  (:import [javax.security.auth Subject])
  (:import [java.security PrivilegedExceptionAction])
  (:import [org.apache.logging.log4j LogManager])
  (:import [org.apache.logging.log4j Level])
  (:import [org.apache.logging.log4j.core.config LoggerConfig])
  (:import [org.apache.storm.generated LogConfig LogLevelAction])
  (:import [org.apache.storm StormTimer])
  (:gen-class))

(defmulti mk-suicide-fn cluster-mode)

(defn read-worker-executors [storm-conf storm-cluster-state storm-id assignment-id port assignment-versions]
  (log-message "Reading Assignments.")
  (let [assignment (:executor->node+port (clojurify-assignment (.assignmentInfo storm-cluster-state storm-id nil)))]
    (doall
     (concat
      [Constants/SYSTEM_EXECUTOR_ID]
      (mapcat (fn [[executor loc]]
                (if (= loc [assignment-id port])
                  [executor]
                  ))
              assignment)))))

(defnk do-executor-heartbeats [worker :executors nil]
  ;; stats is how we know what executors are assigned to this worker 
  (let [stats (if-not executors
                  (into {} (map (fn [e] {e nil}) (:executors worker)))
                  (->> executors
                    (map (fn [e] {(executor/get-executor-id e) (executor/render-stats e)}))
                    (apply merge)))
        zk-hb {:storm-id (:storm-id worker)
               :executor-stats stats
               :uptime (. (:uptime worker) upTime)
               :time-secs (Time/currentTimeSecs)
               }]
    ;; do the zookeeper heartbeat
    (try
      (.workerHeartbeat (:storm-cluster-state worker) (:storm-id worker) (:assignment-id worker) (long (:port worker)) (thriftify-zk-worker-hb zk-hb))
      (catch Exception exc
        (log-error exc "Worker failed to write heatbeats to ZK or Pacemaker...will retry")))))

(defn do-heartbeat [worker]
  (let [conf (:conf worker)
        state (ConfigUtils/workerState conf (:worker-id worker))]
    ;; do the local-file-system heartbeat.
    (.setWorkerHeartBeat state (LSWorkerHeartbeat.
                                 (Time/currentTimeSecs)
                                 (:storm-id worker)
                                 (->ExecutorInfo-list (:executors worker))
                                 (:port worker)))
    (.cleanup state 60) ; this is just in case supervisor is down so that disk doesn't fill up.
                         ; it shouldn't take supervisor 120 seconds between listing dir and reading it

    ))

(defn worker-outbound-tasks
  "Returns seq of task-ids that receive messages from this worker"
  [worker]
  (let [context (worker-context worker)
        components (mapcat
                     (fn [task-id]
                       (->> (.getComponentId context (int task-id))
                            (.getTargets context)
                            vals
                            (map keys)
                            (apply concat)))
                     (:task-ids worker))]
    (-> worker
        :task->component
        (Utils/reverseMap)
        clojurify-structure
        (select-keys components)
        vals
        flatten
        set )))

(defn get-dest
  [^AddressedTuple addressed-tuple]
  "get the destination for an AddressedTuple"
  (.getDest addressed-tuple))

(defn mk-transfer-local-fn [worker]
  (let [short-executor-receive-queue-map (:short-executor-receive-queue-map worker)
        task->short-executor (:task->short-executor worker)
        task-getter (comp #(get task->short-executor %) get-dest)]
    (fn [tuple-batch]
      (let [grouped (fast-group-by task-getter tuple-batch)]
        (fast-map-iter [[short-executor pairs] grouped]
          (let [q (short-executor-receive-queue-map short-executor)]
            (if q
              (.publish ^DisruptorQueue q pairs)
              (log-warn "Received invalid messages for unknown tasks. Dropping... ")
              )))))))

(defn- assert-can-serialize [^KryoTupleSerializer serializer tuple-batch]
  "Check that all of the tuples can be serialized by serializing them."
  (fast-list-iter [[task tuple :as pair] tuple-batch]
    (.serialize serializer tuple)))

(defn- mk-backpressure-handler [executors]
  "make a handler that checks and updates worker's backpressure flag"
  (reify WorkerBackpressureCallback
    (onEvent [this worker]
      (let [storm-id (:storm-id worker)
            assignment-id (:assignment-id worker)
            port (:port worker)
            storm-cluster-state (:storm-cluster-state worker)
            prev-backpressure-flag @(:backpressure worker)]
        (when executors
          (reset! (:backpressure worker)
                  (or @(:transfer-backpressure worker)
                      (reduce #(or %1 %2) (map #(.get-backpressure-flag %1) executors)))))
        ;; update the worker's backpressure flag to zookeeper only when it has changed
        (log-debug "BP " @(:backpressure worker) " WAS " prev-backpressure-flag)
        (when (not= prev-backpressure-flag @(:backpressure worker))
          (try
            (.workerBackpressure storm-cluster-state storm-id assignment-id (long port) @(:backpressure worker))
            (catch Exception exc
              (log-error exc "workerBackpressure update failed when connecting to ZK ... will retry"))))
        ))))

(defn- mk-disruptor-backpressure-handler [worker]
  "make a handler for the worker's send disruptor queue to
  check highWaterMark and lowWaterMark for backpressure"
  (reify DisruptorBackpressureCallback
    (highWaterMark [this]
      (reset! (:transfer-backpressure worker) true)
      (WorkerBackpressureThread/notifyBackpressureChecker (:backpressure-trigger worker)))
    (lowWaterMark [this]
      (reset! (:transfer-backpressure worker) false)
      (WorkerBackpressureThread/notifyBackpressureChecker (:backpressure-trigger worker)))))

(defn mk-transfer-fn [worker]
  (let [local-tasks (-> worker :task-ids set)
        local-transfer (:transfer-local-fn worker)
        ^DisruptorQueue transfer-queue (:transfer-queue worker)
        task->node+port (:cached-task->node+port worker)
        try-serialize-local ((:storm-conf worker) TOPOLOGY-TESTING-ALWAYS-TRY-SERIALIZE)

        transfer-fn
          (fn [^KryoTupleSerializer serializer tuple-batch]
            (let [^ArrayList local (ArrayList.)
                  ^HashMap remoteMap (HashMap.)]
              (fast-list-iter [^AddressedTuple addressed-tuple tuple-batch]
                (let [task (.getDest addressed-tuple)
                      tuple (.getTuple addressed-tuple)]
                  (if (local-tasks task)
                    (.add local addressed-tuple)

                    ;;Using java objects directly to avoid performance issues in java code
                    (do
                      (when (not (.get remoteMap task))
                        (.put remoteMap task (ArrayList.)))
                      (let [^ArrayList remote (.get remoteMap task)]
                        (if (not-nil? task)
                          (.add remote (TaskMessage. task ^bytes (.serialize serializer tuple)))
                          (log-warn "Can't transfer tuple - task value is nil. tuple type: " (pr-str (type tuple)) " and information: " (pr-str tuple)))
                       )))))

              (when (not (.isEmpty local)) (local-transfer local))
              (when (not (.isEmpty remoteMap)) (.publish ^DisruptorQueue transfer-queue remoteMap))))]
    (if try-serialize-local
      (do
        (log-warn "WILL TRY TO SERIALIZE ALL TUPLES (Turn off " TOPOLOGY-TESTING-ALWAYS-TRY-SERIALIZE " for production)")
        (fn [^KryoTupleSerializer serializer tuple-batch]
          (assert-can-serialize serializer tuple-batch)
          (transfer-fn serializer tuple-batch)))
      transfer-fn)))

(defn- mk-receive-queue-map [storm-conf executors]
  (->> executors
       ;; TODO: this depends on the type of executor
       (map (fn [e] [e (DisruptorQueue. (str "receive-queue" e)
                                       (storm-conf TOPOLOGY-EXECUTOR-RECEIVE-BUFFER-SIZE)
                                       (storm-conf TOPOLOGY-DISRUPTOR-WAIT-TIMEOUT-MILLIS)
                                       (storm-conf TOPOLOGY-DISRUPTOR-BATCH-SIZE)
                                       (storm-conf TOPOLOGY-DISRUPTOR-BATCH-TIMEOUT-MILLIS))]))
       (into {})
       ))

(defn- stream->fields [^StormTopology topology component]
  (->> (ThriftTopologyUtils/getComponentCommon topology component)
       .get_streams
       (map (fn [[s info]] [s (Fields. (.get_output_fields info))]))
       (into {})
       (HashMap.)))

(defn component->stream->fields [^StormTopology topology]
  (->> (ThriftTopologyUtils/getComponentIds topology)
       (map (fn [c] [c (stream->fields topology c)]))
       (into {})
       (HashMap.)))

(defn- mk-default-resources [worker]
  (let [conf (:conf worker)
        thread-pool-size (int (conf TOPOLOGY-WORKER-SHARED-THREAD-POOL-SIZE))]
    {WorkerTopologyContext/SHARED_EXECUTOR (Executors/newFixedThreadPool thread-pool-size)}
    ))

(defn- mk-user-resources [worker]
  ;;TODO: need to invoke a hook provided by the topology, giving it a chance to create user resources.
  ;; this would be part of the initialization hook
  ;; need to separate workertopologycontext into WorkerContext and WorkerUserContext.
  ;; actually just do it via interfaces. just need to make sure to hide setResource from tasks
  {})

(defn mk-halting-timer [timer-name]
  (StormTimer. timer-name
    (reify Thread$UncaughtExceptionHandler
      (^void uncaughtException
        [this ^Thread t ^Throwable e]
        (log-error e "Error when processing event")
        (Utils/exitProcess 20 "Error when processing an event")))))

(defn worker-data [conf mq-context storm-id assignment-id port worker-id storm-conf state-store storm-cluster-state]
  (let [assignment-versions (atom {})
        executors (set (read-worker-executors storm-conf storm-cluster-state storm-id assignment-id port assignment-versions))
        transfer-queue (DisruptorQueue. "worker-transfer-queue"
                                                  (storm-conf TOPOLOGY-TRANSFER-BUFFER-SIZE)
                                                  (storm-conf TOPOLOGY-DISRUPTOR-WAIT-TIMEOUT-MILLIS)
                                                  (storm-conf TOPOLOGY-DISRUPTOR-BATCH-SIZE)
                                                  (storm-conf TOPOLOGY-DISRUPTOR-BATCH-TIMEOUT-MILLIS))
        executor-receive-queue-map (mk-receive-queue-map storm-conf executors)

        receive-queue-map (->> executor-receive-queue-map
                               (mapcat (fn [[e queue]] (for [t (executor-id->tasks e)] [t queue])))
                               (into {}))

        topology (ConfigUtils/readSupervisorTopology conf storm-id)
        mq-context  (if mq-context
                      mq-context
                      (TransportFactory/makeContext storm-conf))]

    (recursive-map
      :conf conf
      :mq-context mq-context
      :receiver (.bind ^IContext mq-context storm-id port)
      :storm-id storm-id
      :assignment-id assignment-id
      :port port
      :worker-id worker-id
      :state-store state-store
      :storm-cluster-state storm-cluster-state
      ;; when worker bootup, worker will start to setup initial connections to
      ;; other workers. When all connection is ready, we will enable this flag
      ;; and spout and bolt will be activated.
      :worker-active-flag (atom false)
      :storm-active-atom (atom false)
      :storm-component->debug-atom (atom {})
      :executors executors
      :task-ids (->> receive-queue-map keys (map int) sort)
      :storm-conf storm-conf
      :topology topology
      :system-topology (system-topology! storm-conf topology)
      :heartbeat-timer (mk-halting-timer "heartbeat-timer")
      :refresh-load-timer (mk-halting-timer "refresh-load-timer")
      :refresh-connections-timer (mk-halting-timer "refresh-connections-timer")
      :refresh-credentials-timer (mk-halting-timer "refresh-credentials-timer")
      :reset-log-levels-timer (mk-halting-timer "reset-log-levels-timer")
      :refresh-active-timer (mk-halting-timer "refresh-active-timer")
      :executor-heartbeat-timer (mk-halting-timer "executor-heartbeat-timer")
      :user-timer (mk-halting-timer "user-timer")
      :task->component (HashMap. (storm-task-info topology storm-conf)) ; for optimized access when used in tasks later on
      :component->stream->fields (component->stream->fields (:system-topology <>))
      ;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
      :component->sorted-tasks (->> (:task->component <>) (Utils/reverseMap) (clojurify-structure) (map-val sort))
      :endpoint-socket-lock (ReentrantReadWriteLock.)
      :cached-node+port->socket (atom {})
      :cached-task->node+port (atom {})
      :transfer-queue transfer-queue
      :executor-receive-queue-map executor-receive-queue-map
      ;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
      :short-executor-receive-queue-map (map-key first executor-receive-queue-map)
      :task->short-executor (->> executors
                                 (mapcat (fn [e] (for [t (executor-id->tasks e)] [t (first e)])))
                                 (into {})
                                 (HashMap.))
      :suicide-fn (mk-suicide-fn conf)
      :uptime (Utils/makeUptimeComputer)
      :default-shared-resources (mk-default-resources <>)
      :user-shared-resources (mk-user-resources <>)
      :transfer-local-fn (mk-transfer-local-fn <>)
      :transfer-fn (mk-transfer-fn <>)
      :load-mapping (LoadMapping.)
      :assignment-versions assignment-versions
      :backpressure (atom false) ;; whether this worker is going slow
      :transfer-backpressure (atom false) ;; if the transfer queue is backed-up
      :backpressure-trigger (atom false) ;; a trigger for synchronization with executors
      :throttle-on (atom false) ;; whether throttle is activated for spouts
      )))

(defn- endpoint->string [[node port]]
  (str port "/" node))

(defn string->endpoint [^String s]
  (let [[port-str node] (.split s "/" 2)]
    [node (Integer/valueOf port-str)]
    ))

(def LOAD-REFRESH-INTERVAL-MS 5000)

;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
(defn mk-refresh-load [worker]
  (let [local-tasks (set (:task-ids worker))
        remote-tasks (set/difference (worker-outbound-tasks worker) local-tasks)
        short-executor-receive-queue-map (:short-executor-receive-queue-map worker)
        next-update (atom 0)]
    (fn this
      ([]
        (let [^LoadMapping load-mapping (:load-mapping worker)
              local-pop (map-val (fn [queue]
                                   (let [q-metrics (.getMetrics queue)]
                                     (/ (double (.population q-metrics)) (.capacity q-metrics))))
                                 short-executor-receive-queue-map)
              remote-load (reduce merge (for [[np conn] @(:cached-node+port->socket worker)] (into {} (.getLoad conn remote-tasks))))
              now (System/currentTimeMillis)]
          (.setLocal load-mapping local-pop)
          (.setRemote load-mapping remote-load)
          (when (> now @next-update)
            (.sendLoadMetrics (:receiver worker) local-pop)
            (reset! next-update (+ LOAD-REFRESH-INTERVAL-MS now))))))))

(defmacro read-locked
  [rw-lock & body]
  (let [lock (with-meta rw-lock {:tag `ReentrantReadWriteLock})]
    `(let [rlock# (.readLock ~lock)]
       (try (.lock rlock#)
         ~@body
         (finally (.unlock rlock#))))))

(defmacro write-locked
  [rw-lock & body]
  (let [lock (with-meta rw-lock {:tag `ReentrantReadWriteLock})]
    `(let [wlock# (.writeLock ~lock)]
       (try (.lock wlock#)
         ~@body
         (finally (.unlock wlock#))))))

;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
(defn mk-refresh-connections [worker]
  (let [outbound-tasks (worker-outbound-tasks worker)
        conf (:conf worker)
        storm-cluster-state (:storm-cluster-state worker)
        storm-id (:storm-id worker)]
    (fn refresh-connections
      ([]
        (refresh-connections (fn []
                (.schedule
                  (:refresh-connections-timer worker) 0 refresh-connections))))
      ([callback]
         (let [version (.assignmentVersion storm-cluster-state storm-id callback)
               assignment (if (= version (:version (get @(:assignment-versions worker) storm-id)))
                            (:data (get @(:assignment-versions worker) storm-id))
                            (let [thriftify-assignment-version (.assignmentInfoWithVersion storm-cluster-state storm-id callback)
                              new-assignment {:data (clojurify-assignment (.get thriftify-assignment-version (IStateStorage/DATA))) :version version}]
                              (swap! (:assignment-versions worker) assoc storm-id new-assignment)
                              (:data new-assignment)))
              my-assignment (-> assignment
                                :executor->node+port
                                to-task->node+port
                                (select-keys outbound-tasks)
                                ;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
                                (#(map-val endpoint->string %)))
              ;; we dont need a connection for the local tasks anymore
               ;TODO: when translating this function, you should replace the filter-val with a proper for loop + if condition HERE
              needed-assignment (->> my-assignment
                                      (filter-key (complement (-> worker :task-ids set))))
              needed-connections (-> needed-assignment vals set)
              needed-tasks (-> needed-assignment keys)

              current-connections (set (keys @(:cached-node+port->socket worker)))
              new-connections (set/difference needed-connections current-connections)
              remove-connections (set/difference current-connections needed-connections)]
              (swap! (:cached-node+port->socket worker)
                     #(HashMap. (merge (into {} %1) %2))
                     (into {}
                       (dofor [endpoint-str new-connections
                               :let [[node port] (string->endpoint endpoint-str)]]
                         [endpoint-str
                          (.connect
                           ^IContext (:mq-context worker)
                           storm-id
                           ((:node->host assignment) node)
                           port)
                          ]
                         )))
              (write-locked (:endpoint-socket-lock worker)
                (reset! (:cached-task->node+port worker)
                        (HashMap. my-assignment)))
              (doseq [endpoint remove-connections]
                (.close (get @(:cached-node+port->socket worker) endpoint)))
              (apply swap!
                     (:cached-node+port->socket worker)
                     #(HashMap. (apply dissoc (into {} %1) %&))
                     remove-connections)

           )))))

(defn refresh-storm-active
  ([worker]
    (refresh-storm-active
      worker (fn []
               (.schedule
                 (:refresh-active-timer worker) 0 (partial refresh-storm-active worker)))))
  ([worker callback]
    (let [base (clojurify-storm-base (.stormBase (:storm-cluster-state worker) (:storm-id worker) callback))]
      (reset!
        (:storm-active-atom worker)
        (and (= :active (-> base :status :type)) @(:worker-active-flag worker)))
      (reset! (:storm-component->debug-atom worker) (-> base :component->debug))
      (log-debug "Event debug options " @(:storm-component->debug-atom worker)))))

;; TODO: consider having a max batch size besides what disruptor does automagically to prevent latency issues
(defn mk-transfer-tuples-handler [worker]
   (let [^DisruptorQueue transfer-queue (:transfer-queue worker)
        drainer (TransferDrainer.)
        node+port->socket (:cached-node+port->socket worker)
        task->node+port (:cached-task->node+port worker)
        endpoint-socket-lock (:endpoint-socket-lock worker)
        ]
    (reify com.lmax.disruptor.EventHandler
      (onEvent [this packets seqId batch-end?]
        (.add drainer packets)
        (when batch-end?
          (read-locked endpoint-socket-lock
                       (let [node+port->socket @node+port->socket
                             task->node+port @task->node+port]
                         (.send drainer task->node+port node+port->socket)))
          (.clear drainer))))))

;; Check whether this messaging connection is ready to send data
(defn is-connection-ready [^IConnection connection]
  (if (instance?  ConnectionWithStatus connection)
    (let [^ConnectionWithStatus connection connection
          status (.status connection)]
      (= status ConnectionWithStatus$Status/Ready))
    true))

;; all connections are ready
(defn all-connections-ready [worker]
    (let [connections (vals @(:cached-node+port->socket worker))]
      (every? is-connection-ready connections)))

;; we will wait all connections to be ready and then activate the spout/bolt
;; when the worker bootup
(defn activate-worker-when-all-connections-ready
  [worker]
  (let [timer (:refresh-active-timer worker)
        delay-secs 0
        recur-secs 1]
    (.schedule timer
      delay-secs
      (fn this []
          (if (all-connections-ready worker)
            (do
              (log-message "All connections are ready for worker " (:assignment-id worker) ":" (:port worker)
                " with id " (:worker-id worker))
              (reset! (:worker-active-flag worker) true))
            (.schedule timer recur-secs this false 0))))))

(defn register-callbacks [worker]
  (let [transfer-local-fn (:transfer-local-fn worker)
        ^IConnection socket (:receiver worker)]
    (log-message "Registering IConnectionCallbacks for " (:assignment-id worker) ":" (:port worker))
    (.registerRecv socket (DeserializingConnectionCallback. (:storm-conf worker)
                                                            (worker-context worker)
                                                            transfer-local-fn))))

(defn- close-resources [worker]
  (let [dr (:default-shared-resources worker)]
    (log-message "Shutting down default resources")
    (.shutdownNow (get dr WorkerTopologyContext/SHARED_EXECUTOR))
    (log-message "Shut down default resources")))

(defn- get-logger-levels []
  (into {}
    (let [logger-config (.getConfiguration (LogManager/getContext false))]
      (for [[logger-name logger] (.getLoggers logger-config)]
        {logger-name (.getLevel logger)}))))

(defn set-logger-level [logger-context logger-name new-level]
  (let [config (.getConfiguration logger-context)
        logger-config (.getLoggerConfig config logger-name)]
    (if (not (= (.getName logger-config) logger-name))
      ;; create a new config. Make it additive (true) s.t. inherit
      ;; parents appenders
      (let [new-logger-config (LoggerConfig. logger-name new-level true)]
        (log-message "Adding config for: " new-logger-config " with level: " new-level)
        (.addLogger config logger-name new-logger-config))
      (do
        (log-message "Setting " logger-config " log level to: " new-level)
        (.setLevel logger-config new-level)))))

;; function called on timer to reset log levels last set to DEBUG
;; also called from process-log-config-change
(defn reset-log-levels [latest-log-config-atom]
  (let [latest-log-config @latest-log-config-atom
        logger-context (LogManager/getContext false)]
    (doseq [[logger-name logger-setting] (sort latest-log-config)]
      (let [timeout (:timeout logger-setting)
            target-log-level (:target-log-level logger-setting)
            reset-log-level (:reset-log-level logger-setting)]
        (when (> (coerce/to-long (time/now)) timeout)
          (log-message logger-name ": Resetting level to " reset-log-level) 
          (set-logger-level logger-context logger-name reset-log-level)
          (swap! latest-log-config-atom
            (fn [prev]
              (dissoc prev logger-name))))))
    (.updateLoggers logger-context)))

;; when a new log level is received from zookeeper, this function is called
(defn process-log-config-change [latest-log-config original-log-levels log-config]
  (when log-config
    (log-debug "Processing received log config: " log-config)
    ;; merge log configs together
    (let [loggers (.get_named_logger_level log-config)
          logger-context (LogManager/getContext false)]
      (def new-log-configs
        (into {}
         ;; merge named log levels
         (for [[msg-logger-name logger-level] loggers]
           (let [logger-name (if (= msg-logger-name "ROOT")
                                LogManager/ROOT_LOGGER_NAME
                                msg-logger-name)]
             ;; the new-timeouts map now contains logger => timeout 
             (when (.is_set_reset_log_level_timeout_epoch logger-level)
               {logger-name {:action (.get_action logger-level)
                             :target-log-level (Level/toLevel (.get_target_log_level logger-level))
                             :reset-log-level (or (.get @original-log-levels logger-name) (Level/INFO))
                             :timeout (.get_reset_log_level_timeout_epoch logger-level)}})))))

      ;; look for deleted log timeouts
      (doseq [[logger-name logger-val] (sort @latest-log-config)]
        (when (not (contains? new-log-configs logger-name))
          ;; if we had a timeout, but the timeout is no longer active
          (set-logger-level
            logger-context logger-name (:reset-log-level logger-val))))

      ;; apply new log settings we just received
      ;; the merged configs are only for the reset logic
      (doseq [[msg-logger-name logger-level] (sort (into {} (.get_named_logger_level log-config)))]
        (let [logger-name (if (= msg-logger-name "ROOT")
                                LogManager/ROOT_LOGGER_NAME
                                msg-logger-name)
              level (Level/toLevel (.get_target_log_level logger-level))
              action (.get_action logger-level)]
          (if (= action LogLevelAction/UPDATE)
            (set-logger-level logger-context logger-name level))))
   
      (.updateLoggers logger-context)
      (reset! latest-log-config new-log-configs)
      (log-debug "New merged log config is " @latest-log-config))))

(defn run-worker-start-hooks [worker]
  (let [topology (:topology worker)
        topo-conf (:storm-conf worker)
        worker-topology-context (worker-context worker)
        hooks (.get_worker_hooks topology)]
    (dofor [hook hooks]
      (let [hook-bytes (Utils/toByteArray hook)
            deser-hook (Utils/javaDeserialize hook-bytes BaseWorkerHook)]
        (.start deser-hook topo-conf worker-topology-context)))))

(defn run-worker-shutdown-hooks [worker]
  (let [topology (:topology worker)
        hooks (.get_worker_hooks topology)]
    (dofor [hook hooks]
      (let [hook-bytes (Utils/toByteArray hook)
            deser-hook (Utils/javaDeserialize hook-bytes BaseWorkerHook)]
        (.shutdown deser-hook)))))

;; TODO: should worker even take the storm-id as input? this should be
;; deducable from cluster state (by searching through assignments)
;; what about if there's inconsistency in assignments? -> but nimbus
;; should guarantee this consistency
(defserverfn mk-worker [conf shared-mq-context storm-id assignment-id port worker-id]
  (log-message "Launching worker for " storm-id " on " assignment-id ":" port " with id " worker-id
               " and conf " conf)
  ;; because in local mode, its not a separate
  ;; process. supervisor will register it in this case
  ;; if (ConfigUtils/isLocalMode conf) returns false then it is in distributed mode.
  (when-not (ConfigUtils/isLocalMode conf)
    (SysOutOverSLF4J/sendSystemOutAndErrToSLF4J)
    (let [pid (Utils/processPid)]
      (FileUtils/touch (File. (ConfigUtils/workerPidPath conf worker-id pid)))
      (spit (ConfigUtils/workerArtifactsPidPath conf storm-id port) pid)))

  (declare establish-log-setting-callback)

  ;; start out with empty list of timeouts 
  (def latest-log-config (atom {}))
  (def original-log-levels (atom {}))

  (let [storm-conf (ConfigUtils/readSupervisorStormConf conf storm-id)
        storm-conf (clojurify-structure (ConfigUtils/overrideLoginConfigWithSystemProperty storm-conf))
        acls (Utils/getWorkerACL storm-conf)
        state-store (ClusterUtils/mkStateStorage conf storm-conf  acls  (ClusterStateContext. DaemonType/WORKER))
        storm-cluster-state (ClusterUtils/mkStormClusterState state-store acls (ClusterStateContext.))
        initial-credentials (clojurify-crdentials (.credentials storm-cluster-state storm-id nil))
        auto-creds (AuthUtils/GetAutoCredentials storm-conf)
        subject (AuthUtils/populateSubject nil auto-creds initial-credentials)]
      (Subject/doAs subject (reify PrivilegedExceptionAction
        (run [this]
          (let [worker (worker-data conf shared-mq-context storm-id assignment-id port worker-id storm-conf state-store storm-cluster-state)
        heartbeat-fn #(do-heartbeat worker)

        ;; do this here so that the worker process dies if this fails
        ;; it's important that worker heartbeat to supervisor ASAP when launching so that the supervisor knows it's running (and can move on)
        _ (heartbeat-fn)

        executors (atom nil)
        ;; launch heartbeat threads immediately so that slow-loading tasks don't cause the worker to timeout
        ;; to the supervisor
        _ (.scheduleRecurring (:heartbeat-timer worker) 0 (conf WORKER-HEARTBEAT-FREQUENCY-SECS) heartbeat-fn)

        _ (.scheduleRecurring (:executor-heartbeat-timer worker) 0 (conf TASK-HEARTBEAT-FREQUENCY-SECS)
            (fn [] (do-executor-heartbeats worker :executors @executors)))

        _ (register-callbacks worker)

        refresh-connections (mk-refresh-connections worker)
        refresh-load (mk-refresh-load worker)

        _ (refresh-connections nil)

        _ (activate-worker-when-all-connections-ready worker)

        _ (refresh-storm-active worker nil)

        _ (run-worker-start-hooks worker)

        _ (reset! executors (dofor [e (:executors worker)] (executor/mk-executor worker e initial-credentials)))

        transfer-tuples (mk-transfer-tuples-handler worker)
        
        transfer-thread (Utils/asyncLoop
                          (fn []
                            (.consumeBatchWhenAvailable ^DisruptorQueue (:transfer-queue worker) transfer-tuples) 0))

        disruptor-handler (mk-disruptor-backpressure-handler worker)
        _ (.registerBackpressureCallback (:transfer-queue worker) disruptor-handler)
        _ (-> (.setHighWaterMark (:transfer-queue worker) ((:storm-conf worker) BACKPRESSURE-DISRUPTOR-HIGH-WATERMARK))
              (.setLowWaterMark ((:storm-conf worker) BACKPRESSURE-DISRUPTOR-LOW-WATERMARK))
              (.setEnableBackpressure ((:storm-conf worker) TOPOLOGY-BACKPRESSURE-ENABLE)))
        backpressure-handler (mk-backpressure-handler @executors)        
        backpressure-thread (WorkerBackpressureThread. (:backpressure-trigger worker) worker backpressure-handler)
        _ (if ((:storm-conf worker) TOPOLOGY-BACKPRESSURE-ENABLE) 
            (.start backpressure-thread))
        callback (fn cb []
                   (let [throttle-on (.topologyBackpressure storm-cluster-state storm-id cb)]
                     (reset! (:throttle-on worker) throttle-on)))
        _ (if ((:storm-conf worker) TOPOLOGY-BACKPRESSURE-ENABLE)
            (.topologyBackpressure storm-cluster-state storm-id callback))

        shutdown* (fn []
                    (log-message "Shutting down worker " storm-id " " assignment-id " " port)
                    (doseq [[_ socket] @(:cached-node+port->socket worker)]
                      ;; this will do best effort flushing since the linger period
                      ;; was set on creation
                      (.close socket))
                    (log-message "Terminating messaging context")
                    (log-message "Shutting down executors")
                    (doseq [executor @executors] (.shutdown executor))
                    (log-message "Shut down executors")

                    ;;this is fine because the only time this is shared is when it's a local context,
                    ;;in which case it's a noop
                    (.term ^IContext (:mq-context worker))
                    (log-message "Shutting down transfer thread")
                    (.haltWithInterrupt ^DisruptorQueue (:transfer-queue worker))

                    (.interrupt transfer-thread)
                    (.join transfer-thread)
                    (log-message "Shut down transfer thread")
                    (.terminate backpressure-thread)
                    (log-message "Shut down backpressure thread")
                    (.close (:heartbeat-timer worker))
                    (.close (:refresh-connections-timer worker))
                    (.close (:refresh-credentials-timer worker))
                    (.close (:refresh-active-timer worker))
                    (.close (:executor-heartbeat-timer worker))
                    (.close (:user-timer worker))
                    (.close (:refresh-load-timer worker))
                    (.close (:reset-log-levels-timer worker))
                    (close-resources worker)

                    (log-message "Trigger any worker shutdown hooks")
                    (run-worker-shutdown-hooks worker)

                    (.removeWorkerHeartbeat (:storm-cluster-state worker) storm-id assignment-id (long port))
                    (log-message "Disconnecting from storm cluster state context")
                    (.disconnect (:storm-cluster-state worker))
                    (.close (:state-store worker))
                    (log-message "Shut down worker " storm-id " " assignment-id " " port))
        ret (reify
             Shutdownable
             (shutdown
              [this]
              (shutdown*))
             DaemonCommon
             (waiting? [this]
               (and
                 (.isTimerWaiting (:heartbeat-timer worker))
                 (.isTimerWaiting (:refresh-connections-timer worker))
                 (.isTimerWaiting (:refresh-load-timer worker))
                 (.isTimerWaiting (:refresh-credentials-timer worker))
                 (.isTimerWaiting (:refresh-active-timer worker))
                 (.isTimerWaiting (:executor-heartbeat-timer worker))
                 (.isTimerWaiting (:user-timer worker))
                 ))
             )
        credentials (atom initial-credentials)
        check-credentials-changed (fn []
                                    (let [new-creds (clojurify-crdentials (.credentials (:storm-cluster-state worker) storm-id nil))]
                                      (when-not (= new-creds @credentials) ;;This does not have to be atomic, worst case we update when one is not needed
                                        (AuthUtils/updateSubject subject auto-creds new-creds)
                                        (dofor [e @executors] (.credentials-changed e new-creds))
                                        (reset! credentials new-creds))))
       check-throttle-changed (fn []
                                (let [callback (fn cb []
                                                 (let [throttle-on (.topologyBackpressure (:storm-cluster-state worker) storm-id cb)]
                                                   (reset! (:throttle-on worker) throttle-on)))
                                      new-throttle-on (.topologyBackpressure (:storm-cluster-state worker) storm-id callback)]
                                    (reset! (:throttle-on worker) new-throttle-on)))
        check-log-config-changed (fn []
                                  (let [log-config (.topologyLogConfig (:storm-cluster-state worker) storm-id nil)]
                                    (process-log-config-change latest-log-config original-log-levels log-config)
                                    (establish-log-setting-callback)))]
    (reset! original-log-levels (get-logger-levels))
    (log-message "Started with log levels: " @original-log-levels)
  
    (defn establish-log-setting-callback []
      (.topologyLogConfig (:storm-cluster-state worker) storm-id (fn [args] (check-log-config-changed))))

    (establish-log-setting-callback)
    (clojurify-crdentials (.credentials (:storm-cluster-state worker) storm-id (fn [] (check-credentials-changed))))

    (.scheduleRecurring
      (:refresh-credentials-timer worker) 0 (conf TASK-CREDENTIALS-POLL-SECS)
        (fn []
          (check-credentials-changed)
          (if ((:storm-conf worker) TOPOLOGY-BACKPRESSURE-ENABLE)
            (check-throttle-changed))))
    ;; The jitter allows the clients to get the data at different times, and avoids thundering herd
    (when-not (.get conf TOPOLOGY-DISABLE-LOADAWARE-MESSAGING)
      (.scheduleRecurringWithJitter
        (:refresh-load-timer worker) 0 1 500 refresh-load))
    (.scheduleRecurring
      (:refresh-connections-timer worker) 0 (conf TASK-REFRESH-POLL-SECS) refresh-connections)
    (.scheduleRecurring
      (:reset-log-levels-timer worker) 0 (conf WORKER-LOG-LEVEL-RESET-POLL-SECS)
        (fn [] (reset-log-levels latest-log-config)))
    (.scheduleRecurring
      (:refresh-active-timer worker) 0 (conf TASK-REFRESH-POLL-SECS) (partial refresh-storm-active worker))
    (log-message "Worker has topology config " (Utils/redactValue (:storm-conf worker) STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD))
    (log-message "Worker " worker-id " for storm " storm-id " on " assignment-id ":" port " has finished loading")
    ret
    ))))))

(defmethod mk-suicide-fn
  :local [conf]
  (fn [] (Utils/exitProcess 1 "Worker died")))

(defmethod mk-suicide-fn
  :distributed [conf]
  (fn [] (Utils/exitProcess 1 "Worker died")))

(defn -main [storm-id assignment-id port-str worker-id]
  (let [conf (clojurify-structure (ConfigUtils/readStormConfig))]
    (Utils/setupDefaultUncaughtExceptionHandler)
    (validate-distributed-mode! conf)
    (let [worker (mk-worker conf nil storm-id assignment-id (Integer/parseInt port-str) worker-id)]
      (Utils/addShutdownHookWithForceKillIn1Sec #(.shutdown worker)))))
