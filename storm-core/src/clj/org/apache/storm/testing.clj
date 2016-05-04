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

(ns org.apache.storm.testing
  (:require [org.apache.storm.daemon
             [nimbus :as nimbus]
             [local-supervisor :as local-supervisor]
             [common :as common]
             [worker :as worker]
             [executor :as executor]])
  (:import [org.apache.commons.io FileUtils]
           [org.apache.storm.utils]
           [org.apache.storm.zookeeper Zookeeper]
           [org.apache.storm ProcessSimulator]
           [org.apache.storm.daemon.supervisor StandaloneSupervisor SupervisorData SupervisorManager SupervisorUtils SupervisorManager])
  (:import [java.io File])
  (:import [java.util HashMap ArrayList])
  (:import [java.util.concurrent.atomic AtomicInteger])
  (:import [java.util.concurrent ConcurrentHashMap])
  (:import [org.apache.storm.utils Time Utils IPredicate RegisteredGlobalState ConfigUtils LocalState StormCommonInstaller])
  (:import [org.apache.storm.tuple Fields Tuple TupleImpl])
  (:import [org.apache.storm.task TopologyContext])
  (:import [org.apache.storm.generated GlobalStreamId Bolt KillOptions])
  (:import [org.apache.storm.testing FeederSpout FixedTupleSpout FixedTuple
            TupleCaptureBolt SpoutTracker BoltTracker NonRichBoltTracker
            TestWordSpout MemoryTransactionalSpout])
  (:import [org.apache.storm.security.auth ThriftServer ThriftConnectionType ReqContext AuthUtils])
  (:import [org.apache.storm.generated NotAliveException AlreadyAliveException StormTopology ErrorInfo
            ExecutorInfo InvalidTopologyException Nimbus$Iface Nimbus$Processor SubmitOptions TopologyInitialStatus
            KillOptions RebalanceOptions ClusterSummary SupervisorSummary TopologySummary TopologyInfo
            ExecutorSummary AuthorizationException GetInfoOptions NumErrorsChoice])
  (:import [org.apache.storm.transactional TransactionalSpoutCoordinator])
  (:import [org.apache.storm.transactional.partitioned PartitionedTransactionalSpoutExecutor])
  (:import [org.apache.storm.tuple Tuple])
  (:import [org.apache.storm Thrift])
  (:import [org.apache.storm Config])
  (:import [org.apache.storm.generated StormTopology])
  (:import [org.apache.storm.task TopologyContext]
           (org.apache.storm.messaging IContext)
           [org.json.simple JSONValue]
           (org.apache.storm.daemon StormCommon Acker DaemonCommon))
  (:import [org.apache.storm.cluster ZKStateStorage ClusterStateContext StormClusterStateImpl ClusterUtils])
  (:use [org.apache.storm util config log local-state-converter converter])
  (:use [org.apache.storm.internal thrift]))

(defn feeder-spout
  [fields]
  (FeederSpout. (Fields. fields)))

(defn local-temp-path
  []
  (str (System/getProperty "java.io.tmpdir") (if-not (Utils/isOnWindows) "/") (Utils/uuid)))

(defn delete-all
  [paths]
  (dorun
    (for [t paths]
      (if (.exists (File. t))
        (try
          (FileUtils/forceDelete (File. t))
          (catch Exception e
            (log-message (.getMessage e))))))))

(defmacro with-local-tmp
  [[& tmp-syms] & body]
  (let [tmp-paths (mapcat (fn [t] [t `(local-temp-path)]) tmp-syms)]
    `(let [~@tmp-paths]
       (try
         ~@body
         (finally
           (delete-all ~(vec tmp-syms)))))))

(defn start-simulating-time!
  []
  (Time/startSimulating))

(defn stop-simulating-time!
  []
  (Time/stopSimulating))

(defmacro with-simulated-time
  [& body]
  `(try
    (start-simulating-time!)
    ~@body
    (finally
      (stop-simulating-time!))))

(defn advance-time-ms! [ms]
  (Time/advanceTime ms))

(defn advance-time-secs! [secs]
  (advance-time-ms! (* (long secs) 1000)))

(defn set-var-root*
  [avar val]
  (alter-var-root avar (fn [avar] val)))

(defmacro set-var-root
  [var-sym val]
  `(set-var-root* (var ~var-sym) ~val))

(defmacro with-var-roots
  [bindings & body]
  (let [settings (partition 2 bindings)
        tmpvars (repeatedly (count settings) (partial gensym "old"))
        vars (map first settings)
        savevals (vec (mapcat (fn [t v] [t v]) tmpvars vars))
        setters (for [[v s] settings] `(set-var-root ~v ~s))
        restorers (map (fn [v s] `(set-var-root ~v ~s)) vars tmpvars)]
    `(let ~savevals
       ~@setters
       (try
         ~@body
         (finally
           ~@restorers)))))

(defnk add-supervisor
  [cluster-map :ports 2 :conf {} :id nil]
  (let [tmp-dir (local-temp-path)
        port-ids (if (sequential? ports)
                   ports
                   (doall (repeatedly ports (:port-counter cluster-map))))
        supervisor-conf (merge (:daemon-conf cluster-map)
                               conf
                               {STORM-LOCAL-DIR tmp-dir
                                SUPERVISOR-SLOTS-PORTS port-ids
                                STORM-SUPERVISOR-WORKER-MANAGER-PLUGIN "org.apache.storm.daemon.supervisor.workermanager.DefaultWorkerManager"})
        id-fn (if id id (Utils/uuid))
        isupervisor (proxy [StandaloneSupervisor] []
                        (generateSupervisorId [] id-fn))
        daemon (local-supervisor/mk-local-supervisor supervisor-conf (:shared-context cluster-map) isupervisor)]
    (swap! (:supervisors cluster-map) conj daemon)
    (swap! (:tmp-dirs cluster-map) conj tmp-dir)
    daemon))

(defn mk-shared-context [conf]
  (if-not (conf STORM-LOCAL-MODE-ZMQ)
    (let [context  (org.apache.storm.messaging.local.Context.)]
      (.prepare ^IContext context nil)
      context)))

(defn start-nimbus-daemon [conf nimbus]
  (let [server (ThriftServer. conf (Nimbus$Processor. nimbus)
                              ThriftConnectionType/NIMBUS)
        nimbus-thread (Thread. (fn [] (.serve server)))]
    (log-message "Starting Nimbus server...")
    (.start nimbus-thread)
    server))


(defn- mk-counter
  ([] (mk-counter 1))
  ([start-val]
    (let [val (atom (dec start-val))]
      (fn [] (swap! val inc)))))

;; returns map containing cluster info
;; local dir is always overridden in maps
;; can customize the supervisors (except for ports) by passing in map for :supervisors parameter
;; if need to customize amt of ports more, can use add-supervisor calls afterwards
(defnk mk-local-storm-cluster [:supervisors 2 :ports-per-supervisor 3 :daemon-conf {} :inimbus nil :supervisor-slot-port-min 1024 :nimbus-daemon false]
  (let [zk-tmp (local-temp-path)
        [zk-port zk-handle] (if-not (contains? daemon-conf STORM-ZOOKEEPER-SERVERS)
                              (Zookeeper/mkInprocessZookeeper zk-tmp nil))
        daemon-conf (merge (clojurify-structure (ConfigUtils/readStormConfig))
                           {TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS true
                            ZMQ-LINGER-MILLIS 0
                            TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS false
                            TOPOLOGY-TRIDENT-BATCH-EMIT-INTERVAL-MILLIS 50
                            STORM-CLUSTER-MODE "local"
                            BLOBSTORE-SUPERUSER (System/getProperty "user.name")}
                           (if-not (contains? daemon-conf STORM-ZOOKEEPER-SERVERS)
                             {STORM-ZOOKEEPER-PORT zk-port
                              STORM-ZOOKEEPER-SERVERS ["localhost"]})
                           daemon-conf)
        nimbus-tmp (local-temp-path)
        port-counter (mk-counter supervisor-slot-port-min)
        nimbus (nimbus/service-handler
                (assoc daemon-conf STORM-LOCAL-DIR nimbus-tmp)
                (if inimbus inimbus (nimbus/standalone-nimbus)))
        context (mk-shared-context daemon-conf)
        nimbus-thrift-server (if nimbus-daemon (start-nimbus-daemon daemon-conf nimbus) nil)
        cluster-map {:nimbus nimbus
                     :port-counter port-counter
                     :daemon-conf daemon-conf
                     :supervisors (atom [])
                     :state (ClusterUtils/mkStateStorage daemon-conf nil nil (ClusterStateContext.))
                     :storm-cluster-state (ClusterUtils/mkStormClusterState daemon-conf nil (ClusterStateContext.))
                     :tmp-dirs (atom [nimbus-tmp zk-tmp])
                     :zookeeper (if (not-nil? zk-handle) zk-handle)
                     :shared-context context
                     :nimbus-thrift-server nimbus-thrift-server}
        supervisor-confs (if (sequential? supervisors)
                           supervisors
                           (repeat supervisors {}))]

    (doseq [sc supervisor-confs]
      (add-supervisor cluster-map :ports ports-per-supervisor :conf sc))
    cluster-map))

(defn get-supervisor [cluster-map supervisor-id]
  (let [pred  (reify IPredicate (test [this x] (= (.getId x) supervisor-id)))]
    (Utils/findOne pred @(:supervisors cluster-map))))

(defn remove-first
  [pred aseq]
  (let [[b e] (split-with (complement pred) aseq)]
    (when (empty? e)
      (throw (IllegalArgumentException. "Nothing to remove")))
    (concat b (rest e))))

(defn kill-supervisor [cluster-map supervisor-id]
  (let [finder-fn #(= (.getId %) supervisor-id)
        pred  (reify IPredicate (test [this x] (= (.getId x) supervisor-id)))
        supervisors @(:supervisors cluster-map)
        sup (Utils/findOne pred
                           supervisors)]
    ;; tmp-dir will be taken care of by shutdown
    (reset! (:supervisors cluster-map) (remove-first finder-fn supervisors))
    (.shutdown sup)))

(defn kill-local-storm-cluster [cluster-map]
  (.shutdown (:nimbus cluster-map))
  (if (not-nil? (:nimbus-thrift-server cluster-map))
    (do
      (log-message "shutting down thrift server")
      (try
        (.stop (:nimbus-thrift-server cluster-map))
        (catch Exception e (log-message "failed to stop thrift")))
      ))
  (.close (:state cluster-map))
  (.disconnect (:storm-cluster-state cluster-map))
  (doseq [s @(:supervisors cluster-map)]
    (.shutdownAllWorkers s)
    ;; race condition here? will it launch the workers again?
    (.shutdown s))
  (ProcessSimulator/killAllProcesses)
  (if (not-nil? (:zookeeper cluster-map))
    (do
      (log-message "Shutting down in process zookeeper")
      (Zookeeper/shutdownInprocessZookeeper (:zookeeper cluster-map))
      (log-message "Done shutting down in process zookeeper")))
  (doseq [t @(:tmp-dirs cluster-map)]
    (log-message "Deleting temporary path " t)
    (try
      (Utils/forceDelete t)
      ;; on windows, the host process still holds lock on the logfile
      (catch Exception e (log-message (.getMessage e)))) ))

(def TEST-TIMEOUT-MS
  (let [timeout (System/getenv "STORM_TEST_TIMEOUT_MS")]
    (Integer/parseInt (if timeout timeout "5000"))))

(defmacro while-timeout [timeout-ms condition & body]
  `(let [end-time# (+ (System/currentTimeMillis) ~timeout-ms)]
     (log-debug "Looping until " '~condition)
     (while ~condition
       (when (> (System/currentTimeMillis) end-time#)
         (let [thread-dump# (Utils/threadDump)]
           (log-message "Condition " '~condition  " not met in " ~timeout-ms "ms")
           (log-message thread-dump#)
           (throw (AssertionError. (str "Test timed out (" ~timeout-ms "ms) " '~condition)))))
       ~@body)
     (log-debug "Condition met " '~condition)))

(defn wait-for-condition
  ([apredicate]
    (wait-for-condition TEST-TIMEOUT-MS apredicate))
  ([timeout-ms apredicate]
    (while-timeout timeout-ms (not (apredicate))
      (Time/sleep 100))))
(defn is-supervisor-waiting [^SupervisorManager supervisor]
  (.isWaiting supervisor))

(defn wait-until-cluster-waiting
  "Wait until the cluster is idle. Should be used with time simulation."
  ([cluster-map] (wait-until-cluster-waiting cluster-map TEST-TIMEOUT-MS))
  ([cluster-map timeout-ms]
  ;; wait until all workers, supervisors, and nimbus is waiting
  (let [supervisors @(:supervisors cluster-map)
        workers (filter (partial instance? DaemonCommon) (clojurify-structure (ProcessSimulator/getAllProcessHandles)))
        daemons (concat
                  [(:nimbus cluster-map)]
                  ; because a worker may already be dead
                  workers)]
    (while-timeout timeout-ms (or (not (every? (memfn isWaiting) daemons))
                                (not (every? is-supervisor-waiting supervisors)))
                   (Thread/sleep (rand-int 20))
                   ;;      (doseq [d daemons]
                   ;;        (if-not ((memfn waiting?) d)
                   ;;          (println d)))
                   ))))

(defn advance-cluster-time
  ([cluster-map secs increment-secs]
   (loop [left secs]
     (when (> left 0)
       (let [diff (min left increment-secs)]
         (advance-time-secs! diff)
         (wait-until-cluster-waiting cluster-map)
         (recur (- left diff))))))
  ([cluster-map secs]
   (advance-cluster-time cluster-map secs 1)))

(defmacro with-local-cluster
  [[cluster-sym & args] & body]
  `(let [~cluster-sym (mk-local-storm-cluster ~@args)]
     (try
       ~@body
       (catch Throwable t#
         (log-error t# "Error in cluster")
         (throw t#))
       (finally
         (let [keep-waiting?# (atom true)
               f# (future (while @keep-waiting?# (simulate-wait ~cluster-sym)))]
           (kill-local-storm-cluster ~cluster-sym)
           (reset! keep-waiting?# false)
            @f#)))))

(defmacro with-simulated-time-local-cluster
  [& args]
  `(with-simulated-time
     (with-local-cluster ~@args)))

(defmacro with-inprocess-zookeeper
  [port-sym & body]
  `(with-local-tmp [tmp#]
                   (let [[~port-sym zks#] (Zookeeper/mkInprocessZookeeper tmp# nil)]
                     (try
                       ~@body
                       (finally
                         (Zookeeper/shutdownInprocessZookeeper zks#))))))

(defn submit-local-topology
  [nimbus storm-name conf topology]
  (when-not (Utils/isValidConf conf)
    (throw (IllegalArgumentException. "Topology conf is not json-serializable")))
  (.submitTopology nimbus storm-name nil (JSONValue/toJSONString conf) topology))

(defn submit-local-topology-with-opts
  [nimbus storm-name conf topology submit-opts]
  (when-not (Utils/isValidConf conf)
    (throw (IllegalArgumentException. "Topology conf is not json-serializable")))
  (.submitTopologyWithOpts nimbus storm-name nil (JSONValue/toJSONString conf) topology submit-opts))

(defn mocked-convert-assignments-to-worker->resources [storm-cluster-state storm-name worker->resources]
  (fn [existing-assignments]
    (let [topology-id (StormCommon/getStormId storm-cluster-state storm-name)
          existing-assignments (into {} (for [[tid assignment] existing-assignments]
                                          {tid (:worker->resources assignment)}))
          new-assignments (assoc existing-assignments topology-id worker->resources)]
      new-assignments)))

(defn mocked-compute-new-topology->executor->node+port [storm-cluster-state storm-name executor->node+port]
  (fn [new-scheduler-assignments existing-assignments]
    (let [topology-id (StormCommon/getStormId storm-cluster-state storm-name)
          existing-assignments (into {} (for [[tid assignment] existing-assignments]
                                          {tid (:executor->node+port assignment)}))
          new-assignments (assoc existing-assignments topology-id executor->node+port)]
      new-assignments)))

(defn mocked-compute-new-scheduler-assignments []
  (fn [nimbus existing-assignments topologies scratch-topology-id]
    existing-assignments))

(defn submit-mocked-assignment
  [nimbus storm-cluster-state storm-name conf topology task->component executor->node+port worker->resources]
  (let [fake-common (proxy [StormCommon] []
                      (stormTaskInfoImpl [_] task->component))]
    (with-open [- (StormCommonInstaller. fake-common)]
      (with-var-roots [nimbus/compute-new-scheduler-assignments (mocked-compute-new-scheduler-assignments)
                       nimbus/convert-assignments-to-worker->resources (mocked-convert-assignments-to-worker->resources
                                                              storm-cluster-state
                                                              storm-name
                                                              worker->resources)
                       nimbus/compute-new-topology->executor->node+port (mocked-compute-new-topology->executor->node+port
                                                                          storm-cluster-state
                                                                          storm-name
                                                                          executor->node+port)]
        (submit-local-topology nimbus storm-name conf topology)))))

(defn mk-capture-launch-fn [capture-atom]
  (fn [supervisorData stormId port workerId resources]
    (let [conf (.getConf supervisorData)
          supervisorId (.getSupervisorId supervisorData)
          existing (get @capture-atom [supervisorId port] [])]
      (log-message "mk-capture-launch-fn")
      (ConfigUtils/setWorkerUserWSE conf workerId "")
      (swap! capture-atom assoc [supervisorId port] (conj existing stormId)))))

(defn find-worker-id
  [supervisor-conf port]
  (let [supervisor-state (ConfigUtils/supervisorState supervisor-conf)
        worker->port (.getApprovedWorkers ^LocalState supervisor-state)]
    (first ((clojurify-structure (Utils/reverseMap worker->port)) port))))

(defn find-worker-port
  [supervisor-conf worker-id]
  (let [supervisor-state (ConfigUtils/supervisorState supervisor-conf)
        worker->port (.getApprovedWorkers ^LocalState supervisor-state)]
    (if worker->port (.get worker->port worker-id))))

(defn mk-capture-shutdown-fn
  [capture-atom]
    (fn [supervisorData worker-manager workerId]
      (let [conf (.getConf supervisorData)
            supervisor-id (.getSupervisorId supervisorData)
            port (find-worker-port conf workerId)
            worker-pids (.getWorkerThreadPids supervisorData)
            dead-workers (.getDeadWorkers supervisorData)
            existing (get @capture-atom [supervisor-id port] 0)]
        (log-message "mk-capture-shutdown-fn")
        (swap! capture-atom assoc [supervisor-id port] (inc existing))
        (.shutdownWorker worker-manager supervisor-id workerId worker-pids)
        (if (.cleanupWorker worker-manager workerId)
          (.remove dead-workers workerId)))))
(defmacro capture-changed-workers
  [& body]
  `(let [launch-captured# (atom {})
         shutdown-captured# (atom {})]
     (with-var-roots [local-supervisor/launch-local-worker (mk-capture-launch-fn launch-captured#)
                      local-supervisor/shutdown-local-worker (mk-capture-shutdown-fn shutdown-captured#)]
                     ~@body
                     {:launched @launch-captured#
                      :shutdown @shutdown-captured#})))

(defmacro capture-launched-workers
  [& body]
  `(:launched (capture-changed-workers ~@body)))

(defmacro capture-shutdown-workers
  [& body]
  `(:shutdown (capture-changed-workers ~@body)))

(defnk aggregated-stat
  [cluster-map storm-name stat-key :component-ids nil]
  (let [state (:storm-cluster-state cluster-map)
        nimbus (:nimbus cluster-map)
        storm-id (StormCommon/getStormId state storm-name)
        component->tasks (clojurify-structure (Utils/reverseMap
                           (StormCommon/stormTaskInfo
                             (.getUserTopology nimbus storm-id)
                             (->>
                               (.getTopologyConf nimbus storm-id)
                               (#(if % (JSONValue/parse %)))
                               clojurify-structure))))
        component->tasks (if component-ids
                           (select-keys component->tasks component-ids)
                           component->tasks)
        task-ids (apply concat (vals component->tasks))
        assignment (clojurify-assignment (.assignmentInfo state storm-id nil))
        taskbeats (.taskbeats state storm-id (:task->node+port assignment))
        heartbeats (dofor [id task-ids] (get taskbeats id))
        stats (dofor [hb heartbeats] (if hb (.get (.get hb "stats") stat-key) 0))]
    (reduce + stats)))

(defn emitted-spout-tuples
  [cluster-map topology storm-name]
  (aggregated-stat
    cluster-map
    storm-name
    "emitted"
    :component-ids (keys (.get_spouts topology))))

(defn transferred-tuples
  [cluster-map storm-name]
  (aggregated-stat cluster-map storm-name "transferred"))

(defn acked-tuples
  [cluster-map storm-name]
  (aggregated-stat cluster-map storm-name "acked"))

(defn simulate-wait
  [cluster-map]
  (if (Time/isSimulating)
    (advance-cluster-time cluster-map 10)
    (Thread/sleep 100)))

(defprotocol CompletableSpout
  (exhausted?
    [this]
    "Whether all the tuples for this spout have been completed.")
  (cleanup
    [this]
    "Cleanup any global state kept")
  (startup
    [this]
    "Prepare the spout (globally) before starting the topology"))

(extend-type FixedTupleSpout
  CompletableSpout
  (exhausted? [this]
              (= (-> this .getSourceTuples count)
                 (.getCompleted this)))
  (cleanup [this]
           (.cleanup this))
  (startup [this]))

(extend-type TransactionalSpoutCoordinator
  CompletableSpout
  (exhausted? [this]
              (exhausted? (.getSpout this)))
  (cleanup [this]
           (cleanup (.getSpout this)))
  (startup [this]
           (startup (.getSpout this))))

(extend-type PartitionedTransactionalSpoutExecutor
  CompletableSpout
  (exhausted? [this]
              (exhausted? (.getPartitionedSpout this)))
  (cleanup [this]
           (cleanup (.getPartitionedSpout this)))
  (startup [this]
           (startup (.getPartitionedSpout this))))

(extend-type MemoryTransactionalSpout
  CompletableSpout
  (exhausted? [this]
              (.isExhaustedTuples this))
  (cleanup [this]
           (.cleanup this))
  (startup [this]
           (.startup this)))

(defn spout-objects [spec-map]
  (for [[_ spout-spec] spec-map]
    (-> spout-spec
        .get_spout_object
        (Thrift/deserializeComponentObject))))

(defn capture-topology
  [topology]
  (let [topology (.deepCopy topology)
        spouts (.get_spouts topology)
        bolts (.get_bolts topology)
        all-streams (apply concat
                           (for [[id spec] (merge (clojurify-structure spouts)
                                                  (clojurify-structure bolts))]
                             (for [[stream info] (.. spec get_common get_streams)]
                               [(GlobalStreamId. id stream) (.is_direct info)])))
        capturer (TupleCaptureBolt.)]
    (.set_bolts topology
                (assoc (clojurify-structure bolts)
                  (Utils/uuid)
                  (Bolt.
                    (Thrift/serializeComponentObject capturer)
                    (Thrift/prepareComponentCommon (into {} (for [[id direct?] all-streams]
                                                          [id (if direct?
                                                                (Thrift/prepareDirectGrouping)
                                                                (Thrift/prepareGlobalGrouping))]))
                                               {}
                                               nil))))
    {:topology topology
     :capturer capturer}))

;; TODO: mock-sources needs to be able to mock out state spouts as well
;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
(defnk complete-topology
  [cluster-map topology
   :mock-sources {}
   :storm-conf {}
   :cleanup-state true
   :topology-name nil
   :timeout-ms TEST-TIMEOUT-MS]
  ;; TODO: the idea of mocking for transactional topologies should be done an
  ;; abstraction level above... should have a complete-transactional-topology for this
  (let [{topology :topology capturer :capturer} (capture-topology topology)
        storm-name (or topology-name (str "topologytest-" (Utils/uuid)))
        state (:storm-cluster-state cluster-map)
        spouts (.get_spouts topology)
        replacements (map-val (fn [v]
                                (FixedTupleSpout.
                                  (for [tup v]
                                    (if (map? tup)
                                      (FixedTuple. (:stream tup) (:values tup))
                                      tup))))
                              mock-sources)]
    (doseq [[id spout] replacements]
      (let [spout-spec (get spouts id)]
        (.set_spout_object spout-spec (Thrift/serializeComponentObject spout))))
    (doseq [spout (spout-objects spouts)]
      (when-not (extends? CompletableSpout (.getClass spout))
        (throw (RuntimeException. (str "Cannot complete topology unless every spout is a CompletableSpout (or mocked to be); failed by " spout)))))

    (doseq [spout (spout-objects spouts)]
      (startup spout))

    (submit-local-topology (:nimbus cluster-map) storm-name storm-conf topology)
    (advance-cluster-time cluster-map 11)

    (let [storm-id (StormCommon/getStormId state storm-name)]
      ;;Give the topology time to come up without using it to wait for the spouts to complete
      (simulate-wait cluster-map)

      (while-timeout timeout-ms (not (every? exhausted? (spout-objects spouts)))
                     (simulate-wait cluster-map))

      (.killTopologyWithOpts (:nimbus cluster-map) storm-name (doto (KillOptions.) (.set_wait_secs 0)))
      (while-timeout timeout-ms (clojurify-assignment (.assignmentInfo state storm-id nil))
                     (simulate-wait cluster-map))
      (when cleanup-state
        (doseq [spout (spout-objects spouts)]
          (cleanup spout))))

    (if cleanup-state
      (.getAndRemoveResults capturer)
      (.getAndClearResults capturer))))

(defn read-tuples
  ([results component-id stream-id]
   (let [fixed-tuples (get results component-id [])]
     (mapcat
       (fn [ft]
         (if (= stream-id (. ft stream))
           [(vec (. ft values))]))
       fixed-tuples)
     ))
  ([results component-id]
   (read-tuples results component-id Utils/DEFAULT_STREAM_ID)))

(defn multi-set
  "Returns a map of elem to count"
  [aseq]
  (apply merge-with +
         (map #(hash-map % 1) aseq)))

(defn ms=
  [& args]
  (apply = (map multi-set args)))

(def TRACKER-BOLT-ID "+++tracker-bolt")

;; TODO: should override system-topology! and wrap everything there
(defn mk-tracked-topology
  ([tracked-cluster topology]
   (let [track-id (::track-id tracked-cluster)
         ret (.deepCopy topology)]
     (dofor [[_ bolt] (.get_bolts ret)
             :let [obj (Thrift/deserializeComponentObject (.get_bolt_object bolt))]]
            (.set_bolt_object bolt (Thrift/serializeComponentObject
                                     (BoltTracker. obj track-id))))
     (dofor [[_ spout] (.get_spouts ret)
             :let [obj (Thrift/deserializeComponentObject (.get_spout_object spout))]]
            (.set_spout_object spout (Thrift/serializeComponentObject
                                       (SpoutTracker. obj track-id))))
     {:topology ret
      :last-spout-emit (atom 0)
      :cluster tracked-cluster})))

(defn assoc-track-id
  [cluster track-id]
  (assoc cluster ::track-id track-id))

(defn increment-global!
  [id key amt]
  (-> (RegisteredGlobalState/getState id)
      (get key)
      (.addAndGet amt)))

(defn global-amt
  [id key]
  (-> (RegisteredGlobalState/getState id)
      (get key)
      .get))

(defmacro with-tracked-cluster
  [[cluster-sym & cluster-args] & body]
  `(let [id# (Utils/uuid)
         fake-common# (proxy [StormCommon] []
                        (makeAckerBoltImpl [] (let [tracker-acker# (NonRichBoltTracker. (Acker.) (String. id#))]
                                                tracker-acker#)))]
    (with-open [-# (StormCommonInstaller. fake-common#)]
      (RegisteredGlobalState/setState
        id#
        (doto (ConcurrentHashMap.)
          (.put "spout-emitted" (AtomicInteger. 0))
          (.put "transferred" (AtomicInteger. 0))
          (.put "processed" (AtomicInteger. 0))))
      (with-var-roots
        [;; critical that this particular function is overridden here,
         ;; since the transferred stat needs to be incremented at the moment
         ;; of tuple emission (and not on a separate thread later) for
         ;; topologies to be tracked correctly. This is because "transferred" *must*
         ;; be incremented before "processing".
         executor/mk-executor-transfer-fn
         (let [old# executor/mk-executor-transfer-fn]
           (fn [& args#]
             (let [transferrer# (apply old# args#)]
               (fn [& args2#]
                 ;; (log-message "Transferring: " transfer-args#)
                 (increment-global! id# "transferred" 1)
                 (apply transferrer# args2#)))))]
          (with-simulated-time-local-cluster [~cluster-sym ~@cluster-args]
                              (let [~cluster-sym (assoc-track-id ~cluster-sym id#)]
                                ~@body)))
      (RegisteredGlobalState/clearState id#))))

(defn tracked-wait
  "Waits until topology is idle and 'amt' more tuples have been emitted by spouts."
  ([tracked-topology]
     (tracked-wait tracked-topology 1 TEST-TIMEOUT-MS))
  ([tracked-topology amt]
     (tracked-wait tracked-topology amt TEST-TIMEOUT-MS))
  ([tracked-topology amt timeout-ms]
    (let [target (+ amt @(:last-spout-emit tracked-topology))
          track-id (-> tracked-topology :cluster ::track-id)
          waiting? (fn []
                     (or (not= target (global-amt track-id "spout-emitted"))
                         (not= (global-amt track-id "transferred")
                               (global-amt track-id "processed"))))]
      (while-timeout timeout-ms (waiting?)
                     ;; (println "Spout emitted: " (global-amt track-id "spout-emitted"))
                     ;; (println "Processed: " (global-amt track-id "processed"))
                     ;; (println "Transferred: " (global-amt track-id "transferred"))
                    (Thread/sleep (rand-int 200)))
      (reset! (:last-spout-emit tracked-topology) target))))

(defnk test-tuple
  [values
   :stream Utils/DEFAULT_STREAM_ID
   :component "component"
   :fields nil]
  (let [fields (or fields
                   (->> (iterate inc 1)
                        (take (count values))
                        (map #(str "field" %))))
        spout-spec (Thrift/prepareSerializedSpoutDetails
                     (TestWordSpout.)
                     {stream fields})
        topology (StormTopology. {component spout-spec} {} {})
        context (TopologyContext.
                  topology
                  (clojurify-structure (ConfigUtils/readStormConfig))
                  {(int 1) component}
                  {component [(int 1)]}
                  {component {stream (Fields. fields)}}
                  "test-storm-id"
                  nil
                  nil
                  (int 1)
                  nil
                  [(int 1)]
                  {}
                  {}
                  (HashMap.)
                  (HashMap.)
                  (atom false))]
    (TupleImpl. context values 1 stream)))

(defmacro with-timeout
  [millis unit & body]
  `(let [f# (future ~@body)]
     (try
       (.get f# ~millis ~unit)
       (finally (future-cancel f#)))))
