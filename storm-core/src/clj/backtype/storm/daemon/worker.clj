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
(ns backtype.storm.daemon.worker
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap])
  (:require [backtype.storm.daemon [executor :as executor]])
  (:import [java.util.concurrent Executors])
  (:import [backtype.storm.messaging TransportFactory])
  (:import [backtype.storm.messaging IContext IConnection])
  (:gen-class))

(bootstrap)

(defmulti mk-suicide-fn cluster-mode)

(defn read-worker-executors [storm-conf storm-cluster-state storm-id assignment-id port]
  (let [assignment (:executor->node+port (.assignment-info storm-cluster-state storm-id nil))]
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
               :uptime ((:uptime worker))
               :time-secs (current-time-secs)
               }]
    ;; do the zookeeper heartbeat
    (.worker-heartbeat! (:storm-cluster-state worker) (:storm-id worker) (:assignment-id worker) (:port worker) zk-hb)    
    ))

(defn do-heartbeat [worker]
  (let [conf (:conf worker)
        hb (WorkerHeartbeat.
             (current-time-secs)
             (:storm-id worker)
             (:executors worker)
             (:port worker))
        state (worker-state conf (:worker-id worker))]
    (log-debug "Doing heartbeat " (pr-str hb))
    ;; do the local-file-system heartbeat.
    (.put state
        LS-WORKER-HEARTBEAT
        hb
        false
        )
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
        reverse-map
        (select-keys components)
        vals
        flatten
        set )))

(defn mk-transfer-local-fn [worker]
  (let [short-executor-receive-queue-map (:short-executor-receive-queue-map worker)
        task->short-executor (:task->short-executor worker)
        task-getter (comp #(get task->short-executor %) fast-first)]
    (fn [tuple-batch]
      (let [grouped (fast-group-by task-getter tuple-batch)]
        (fast-map-iter [[short-executor pairs] grouped]
          (let [q (short-executor-receive-queue-map short-executor)]
            (if q
              (disruptor/publish q pairs)
              (log-warn "Received invalid messages for unknown tasks. Dropping... ")
              )))))))

(defn mk-transfer-fn [worker]
  (let [local-tasks (-> worker :task-ids set)
        local-transfer (:transfer-local-fn worker)
        ^DisruptorQueue transfer-queue (:transfer-queue worker)]
    (fn [^KryoTupleSerializer serializer tuple-batch]
      (let [local (ArrayList.)
            remote (ArrayList.)]
        (fast-list-iter [[task tuple :as pair] tuple-batch]
          (if (local-tasks task)
            (.add local pair)
            (.add remote pair)
            ))
        (local-transfer local)
        ;; not using map because the lazy seq shows up in perf profiles
        (let [serialized-pairs (fast-list-for [[task ^TupleImpl tuple] remote] [task (.serialize serializer tuple)])]
          (disruptor/publish transfer-queue serialized-pairs)
          )))))

(defn- mk-receive-queue-map [storm-conf executors]
  (->> executors
       ;; TODO: this depends on the type of executor
       (map (fn [e] [e (disruptor/disruptor-queue (storm-conf TOPOLOGY-EXECUTOR-RECEIVE-BUFFER-SIZE)
                                                  :wait-strategy (storm-conf TOPOLOGY-DISRUPTOR-WAIT-STRATEGY))]))
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

(defn mk-halting-timer []
  (mk-timer :kill-fn (fn [t]
                       (log-error t "Error when processing event")
                       (halt-process! 20 "Error when processing an event")
                       )))

(defn worker-data [conf mq-context storm-id assignment-id port worker-id]
  (let [cluster-state (cluster/mk-distributed-cluster-state conf)
        storm-cluster-state (cluster/mk-storm-cluster-state cluster-state)
        storm-conf (read-supervisor-storm-conf conf storm-id)
        executors (set (read-worker-executors storm-conf storm-cluster-state storm-id assignment-id port))
        transfer-queue (disruptor/disruptor-queue (storm-conf TOPOLOGY-TRANSFER-BUFFER-SIZE)
                                                  :wait-strategy (storm-conf TOPOLOGY-DISRUPTOR-WAIT-STRATEGY))
        executor-receive-queue-map (mk-receive-queue-map storm-conf executors)
        
        receive-queue-map (->> executor-receive-queue-map
                               (mapcat (fn [[e queue]] (for [t (executor-id->tasks e)] [t queue])))
                               (into {}))

        topology (read-supervisor-topology conf storm-id)]
    (recursive-map
      :conf conf
      :mq-context (if mq-context
                      mq-context
                      (TransportFactory/makeContext storm-conf))
      :storm-id storm-id
      :assignment-id assignment-id
      :port port
      :worker-id worker-id
      :cluster-state cluster-state
      :storm-cluster-state storm-cluster-state
      :storm-active-atom (atom false)
      :executors executors
      :task-ids (->> receive-queue-map keys (map int) sort)
      :storm-conf storm-conf
      :topology topology
      :system-topology (system-topology! storm-conf topology)
      :heartbeat-timer (mk-halting-timer)
      :refresh-connections-timer (mk-halting-timer)
      :refresh-active-timer (mk-halting-timer)
      :executor-heartbeat-timer (mk-halting-timer)
      :user-timer (mk-halting-timer)
      :task->component (HashMap. (storm-task-info topology storm-conf)) ; for optimized access when used in tasks later on
      :component->stream->fields (component->stream->fields (:system-topology <>))
      :component->sorted-tasks (->> (:task->component <>) reverse-map (map-val sort))
      :endpoint-socket-lock (mk-rw-lock)
      :cached-node+port->socket (atom {})
      :cached-task->node+port (atom {})
      :transfer-queue transfer-queue
      :executor-receive-queue-map executor-receive-queue-map
      :short-executor-receive-queue-map (map-key first executor-receive-queue-map)
      :task->short-executor (->> executors
                                 (mapcat (fn [e] (for [t (executor-id->tasks e)] [t (first e)])))
                                 (into {})
                                 (HashMap.))
      :suicide-fn (mk-suicide-fn conf)
      :uptime (uptime-computer)
      :default-shared-resources (mk-default-resources <>)
      :user-shared-resources (mk-user-resources <>)
      :transfer-local-fn (mk-transfer-local-fn <>)
      :transfer-fn (mk-transfer-fn <>)
      )))

(defn- endpoint->string [[node port]]
  (str port "/" node))

(defn string->endpoint [^String s]
  (let [[port-str node] (.split s "/" 2)]
    [node (Integer/valueOf port-str)]
    ))

(defn mk-refresh-connections [worker]
  (let [outbound-tasks (worker-outbound-tasks worker)
        conf (:conf worker)
        storm-cluster-state (:storm-cluster-state worker)
        storm-id (:storm-id worker)]
    (fn this
      ([]
        (this (fn [& ignored] (schedule (:refresh-connections-timer worker) 0 this))))
      ([callback]
        (let [assignment (.assignment-info storm-cluster-state storm-id callback)
              my-assignment (-> assignment
                                :executor->node+port
                                to-task->node+port
                                (select-keys outbound-tasks)
                                (#(map-val endpoint->string %)))
              ;; we dont need a connection for the local tasks anymore
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
              
              (let [missing-tasks (->> needed-tasks
                                       (filter (complement my-assignment)))]
                (when-not (empty? missing-tasks)
                  (log-warn "Missing assignment for following tasks: " (pr-str missing-tasks))
                  )))))))

(defn refresh-storm-active
  ([worker]
    (refresh-storm-active worker (fn [& ignored] (schedule (:refresh-active-timer worker) 0 (partial refresh-storm-active worker)))))
  ([worker callback]
    (let [base (.storm-base (:storm-cluster-state worker) (:storm-id worker) callback)]
     (reset!
      (:storm-active-atom worker)
      (= :active (-> base :status :type))
      ))
     ))

;; TODO: consider having a max batch size besides what disruptor does automagically to prevent latency issues
(defn mk-transfer-tuples-handler [worker]
  (let [^DisruptorQueue transfer-queue (:transfer-queue worker)
        drainer (ArrayList.)
        node+port->socket (:cached-node+port->socket worker)
        task->node+port (:cached-task->node+port worker)
        endpoint-socket-lock (:endpoint-socket-lock worker)
        ]
    (disruptor/clojure-handler
      (fn [packets _ batch-end?]
        (.addAll drainer packets)
        (when batch-end?
          (read-locked endpoint-socket-lock
            (let [node+port->socket @node+port->socket
                  task->node+port @task->node+port]
              ;; consider doing some automatic batching here (would need to not be serialized at this point to remove per-tuple overhead)
              ;; try using multipart messages ... first sort the tuples by the target node (without changing the local ordering)
            
              (fast-list-iter [[task ser-tuple] drainer]
                ;; TODO: consider write a batch of tuples here to every target worker  
                ;; group by node+port, do multipart send              
                (let [node-port (get task->node+port task)]
                  (when node-port
                    (.send ^IConnection (get node+port->socket node-port) task ser-tuple))
                    ))))
          (.clear drainer))))))

(defn launch-receive-thread [worker]
  (log-message "Launching receive-thread for " (:assignment-id worker) ":" (:port worker))
  (msg-loader/launch-receive-thread!
    (:mq-context worker)
    (:storm-id worker)
    (:port worker)
    (:transfer-local-fn worker)
    (-> worker :storm-conf (get TOPOLOGY-RECEIVER-BUFFER-SIZE))
    :kill-fn (fn [t] (halt-process! 11))))

(defn- close-resources [worker]
  (let [dr (:default-shared-resources worker)]
    (log-message "Shutting down default resources")
    (.shutdownNow (get dr WorkerTopologyContext/SHARED_EXECUTOR))
    (log-message "Shut down default resources")))

;; TODO: should worker even take the storm-id as input? this should be
;; deducable from cluster state (by searching through assignments)
;; what about if there's inconsistency in assignments? -> but nimbus
;; should guarantee this consistency
;; TODO: consider doing worker heartbeating rather than task heartbeating to reduce the load on zookeeper
(defserverfn mk-worker [conf shared-mq-context storm-id assignment-id port worker-id]
  (log-message "Launching worker for " storm-id " on " assignment-id ":" port " with id " worker-id
               " and conf " conf)
  (if-not (local-mode? conf)
    (redirect-stdio-to-slf4j!))
  ;; because in local mode, its not a separate
  ;; process. supervisor will register it in this case
  (when (= :distributed (cluster-mode conf))
    (touch (worker-pid-path conf worker-id (process-pid))))
  (let [worker (worker-data conf shared-mq-context storm-id assignment-id port worker-id)
        heartbeat-fn #(do-heartbeat worker)
        ;; do this here so that the worker process dies if this fails
        ;; it's important that worker heartbeat to supervisor ASAP when launching so that the supervisor knows it's running (and can move on)
        _ (heartbeat-fn)
        
        ;; heartbeat immediately to nimbus so that it knows that the worker has been started
        _ (do-executor-heartbeats worker)
        
        
        executors (atom nil)
        ;; launch heartbeat threads immediately so that slow-loading tasks don't cause the worker to timeout
        ;; to the supervisor
        _ (schedule-recurring (:heartbeat-timer worker) 0 (conf WORKER-HEARTBEAT-FREQUENCY-SECS) heartbeat-fn)
        _ (schedule-recurring (:executor-heartbeat-timer worker) 0 (conf TASK-HEARTBEAT-FREQUENCY-SECS) #(do-executor-heartbeats worker :executors @executors))

        
        refresh-connections (mk-refresh-connections worker)

        _ (refresh-connections nil)
        _ (refresh-storm-active worker nil)
 
        _ (reset! executors (dofor [e (:executors worker)] (executor/mk-executor worker e)))
        receive-thread-shutdown (launch-receive-thread worker)
        
        transfer-tuples (mk-transfer-tuples-handler worker)
        
        transfer-thread (disruptor/consume-loop* (:transfer-queue worker) transfer-tuples)                                       
        shutdown* (fn []
                    (log-message "Shutting down worker " storm-id " " assignment-id " " port)
                    (doseq [[_ socket] @(:cached-node+port->socket worker)]
                      ;; this will do best effort flushing since the linger period
                      ;; was set on creation
                      (.close socket))
                    (log-message "Shutting down receive thread")
                    (receive-thread-shutdown)
                    (log-message "Shut down receive thread")
                    (log-message "Terminating messaging context")
                    (log-message "Shutting down executors")
                    (doseq [executor @executors] (.shutdown executor))
                    (log-message "Shut down executors")
                                        
                    ;;this is fine because the only time this is shared is when it's a local context,
                    ;;in which case it's a noop
                    (.term ^IContext (:mq-context worker))
                    (log-message "Shutting down transfer thread")
                    (disruptor/halt-with-interrupt! (:transfer-queue worker))

                    (.interrupt transfer-thread)
                    (.join transfer-thread)
                    (log-message "Shut down transfer thread")
                    (cancel-timer (:heartbeat-timer worker))
                    (cancel-timer (:refresh-connections-timer worker))
                    (cancel-timer (:refresh-active-timer worker))
                    (cancel-timer (:executor-heartbeat-timer worker))
                    (cancel-timer (:user-timer worker))
                    
                    (close-resources worker)
                    
                    ;; TODO: here need to invoke the "shutdown" method of WorkerHook
                    
                    (.remove-worker-heartbeat! (:storm-cluster-state worker) storm-id assignment-id port)
                    (log-message "Disconnecting from storm cluster state context")
                    (.disconnect (:storm-cluster-state worker))
                    (.close (:cluster-state worker))
                    (log-message "Shut down worker " storm-id " " assignment-id " " port))
        ret (reify
             Shutdownable
             (shutdown
              [this]
              (shutdown*))
             DaemonCommon
             (waiting? [this]
               (and
                 (timer-waiting? (:heartbeat-timer worker))
                 (timer-waiting? (:refresh-connections-timer worker))
                 (timer-waiting? (:refresh-active-timer worker))
                 (timer-waiting? (:executor-heartbeat-timer worker))
                 (timer-waiting? (:user-timer worker))
                 ))
             )]
    
    (schedule-recurring (:refresh-connections-timer worker) 0 (conf TASK-REFRESH-POLL-SECS) refresh-connections)
    (schedule-recurring (:refresh-active-timer worker) 0 (conf TASK-REFRESH-POLL-SECS) (partial refresh-storm-active worker))

    (log-message "Worker has topology config " (:storm-conf worker))
    (log-message "Worker " worker-id " for storm " storm-id " on " assignment-id ":" port " has finished loading")
    ret
    ))

(defmethod mk-suicide-fn
  :local [conf]
  (fn [] (halt-process! 1 "Worker died")))

(defmethod mk-suicide-fn
  :distributed [conf]
  (fn [] (halt-process! 1 "Worker died")))

(defn -main [storm-id assignment-id port-str worker-id]  
  (let [conf (read-storm-config)]
    (validate-distributed-mode! conf)
    (mk-worker conf nil storm-id assignment-id (Integer/parseInt port-str) worker-id)))
