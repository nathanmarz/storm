(ns backtype.storm.daemon.worker
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap])
  (:require [backtype.storm.daemon [executor :as executor]])
  (:gen-class))

(bootstrap)

(defmulti mk-suicide-fn cluster-mode)

(defn read-worker-executors [storm-cluster-state storm-id supervisor-id port]
  (let [assignment (:executor->node+port (.assignment-info storm-cluster-state storm-id nil))]
    (doall
      (mapcat (fn [[executor loc]]
              (if (= loc [supervisor-id port])
                [executor]
                ))
            assignment))
    ))

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
    (.worker-heartbeat! (:storm-cluster-state worker) (:storm-id worker) (:supervisor-id worker) (:port worker) zk-hb)    
    ))

(defn do-heartbeat [worker]
  (let [conf (:conf worker)
        hb (WorkerHeartbeat.
             (current-time-secs)
             (:storm-id worker)
             (:executors worker)
             (:port worker))]
    (log-debug "Doing heartbeat " (pr-str hb))
    ;; do the local-file-system heartbeat.
    (.put (worker-state conf (:worker-id worker))
        LS-WORKER-HEARTBEAT
        hb)
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
        task->short-executor (:task->short-executor worker)]
    (fn [tuple-batch]
      (let [grouped (group-by (comp task->short-executor first) tuple-batch)]
        ;;TODO: Need a fast map iter
        (fast-map-iter [[short-executor pairs] grouped]
          (let [q (short-executor-receive-queue-map short-executor)]
            (if q
              (disruptor/publish q pairs)
              (log-warn "Received invalid messages for unknown tasks. Dropping... " (pr-str tuple-batch))
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
       (map (fn [e] [e (disruptor/disruptor-queue (storm-conf TOPOLOGY-EXECUTOR-RECEIVE-BUFFER-SIZE))]))
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

(defn worker-data [conf mq-context storm-id supervisor-id port worker-id]
  (let [cluster-state (cluster/mk-distributed-cluster-state conf)
        storm-cluster-state (cluster/mk-storm-cluster-state cluster-state)
        storm-conf (read-supervisor-storm-conf conf storm-id)
        executors (set (read-worker-executors storm-cluster-state storm-id supervisor-id port))
        transfer-queue (disruptor/disruptor-queue (storm-conf TOPOLOGY-TRANSFER-BUFFER-SIZE))
        executor-receive-queue-map (mk-receive-queue-map storm-conf executors)
        
        receive-queue-map (->> executor-receive-queue-map
                               (mapcat (fn [[e queue]] (for [t (executor-id->tasks e)] [t queue])))
                               (into {}))

        topology (read-supervisor-topology conf storm-id)]
    (recursive-map
      :conf conf
      :mq-context (if mq-context
                      mq-context
                      (msg-loader/mk-zmq-context (storm-conf ZMQ-THREADS)
                                                 (storm-conf ZMQ-LINGER-MILLIS)
                                                 (= (conf STORM-CLUSTER-MODE) "local")))
      :storm-id storm-id
      :supervisor-id supervisor-id
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
      :timer (mk-timer :kill-fn (fn [t]
                                  (log-error t "Error when processing event")
                                  (halt-process! 20 "Error when processing an event")
                                  ))
      :task->component (HashMap. (storm-task-info topology storm-conf)) ; for optimized access when used in tasks later on
      :component->stream->fields (component->stream->fields (:system-topology <>))
      :component->sorted-tasks (->> (:task->component <>) reverse-map (map-val sort))
      :endpoint-socket-lock (mk-rw-lock)
      :node+port->socket (atom {})
      :task->node+port (atom {})
      :transfer-queue transfer-queue
      :executor-receive-queue-map executor-receive-queue-map
      :short-executor-receive-queue-map (map-key first executor-receive-queue-map)
      :task->short-executor (->> executors
                                 (mapcat (fn [e] (for [t (executor-id->tasks e)] [t (first e)])))
                                 (into {}))
      :suicide-fn (mk-suicide-fn conf)
      :uptime (uptime-computer)
      :transfer-local-fn (mk-transfer-local-fn <>)
      :transfer-fn (mk-transfer-fn <>)
      )))

(defn mk-refresh-connections [worker]
  (let [outbound-tasks (worker-outbound-tasks worker)
        conf (:conf worker)
        storm-cluster-state (:storm-cluster-state worker)
        storm-id (:storm-id worker)]
    (fn this
      ([]
        (this (fn [& ignored] (schedule (:timer worker) 0 this))))
      ([callback]
        (let [assignment (.assignment-info storm-cluster-state storm-id callback)
              my-assignment (-> assignment :executor->node+port to-task->node+port (select-keys outbound-tasks))
              ;; we dont need a connection for the local tasks anymore
              needed-connections (->> my-assignment
                                      (filter-key (complement (-> worker :task-ids set)))
                                      vals
                                      set)
              current-connections (set (keys @(:node+port->socket worker)))
              new-connections (set/difference needed-connections current-connections)
              remove-connections (set/difference current-connections needed-connections)]
              (swap! (:node+port->socket worker)
                     merge
                     (into {}
                       (dofor [[node port :as endpoint] new-connections]
                         [endpoint
                          (msg/connect
                           (:mq-context worker)
                           storm-id
                           ((:node->host assignment) node)
                           port)
                          ]
                         )))
              (write-locked (:endpoint-socket-lock worker)
                (reset! (:task->node+port worker) my-assignment))
              (doseq [endpoint remove-connections]
                (.close (@(:node+port->socket worker) endpoint)))
              (apply swap!
                     (:node+port->socket worker)
                     dissoc
                     remove-connections)
          )))))

(defn refresh-storm-active
  ([worker]
    (refresh-storm-active worker (fn [& ignored] (schedule (:timer worker) 0 (partial refresh-storm-active worker)))))
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
        node+port->socket (:node+port->socket worker)
        task->node+port (:task->node+port worker)
        endpoint-socket-lock (:endpoint-socket-lock worker)
        ]
    (fn [packets _ batch-end?]
      (.addAll drainer packets)
      (when batch-end?
        (read-locked endpoint-socket-lock
          (let [node+port->socket @node+port->socket
                task->node+port @task->node+port]
            ;; consider doing some automatic batching here (would need to not be serialized at this point to remove per-tuple overhead)
            ;; try using multipart messages ... first sort the tuples by the target node (without changing the local ordering)
            
            (fast-list-iter [[task ser-tuple] drainer]
              ;; TODO: this lookup (with the vector is expensive)
              ;; TODO: write a batch of tuples here to every target worker  
              ;; group by node+port, do multipart send              
              (let [socket (node+port->socket (task->node+port task))]
                (msg/send socket task ser-tuple)
                ))))
        (.clear drainer)))))

(defn launch-receive-thread [worker]
  (log-message "Launching receive-thread for " (:supervisor-id worker) ":" (:port worker))
  (msg-loader/launch-receive-thread!
    (:mq-context worker)
    (:storm-id worker)
    (:port worker)
    (:transfer-local-fn worker)
    (-> worker :storm-conf (get TOPOLOGY-RECEIVER-BUFFER-SIZE))
    :kill-fn (fn [t] (halt-process! 11))))

;; TODO: should worker even take the storm-id as input? this should be
;; deducable from cluster state (by searching through assignments)
;; what about if there's inconsistency in assignments? -> but nimbus
;; should guarantee this consistency
;; TODO: consider doing worker heartbeating rather than task heartbeating to reduce the load on zookeeper
(defserverfn mk-worker [conf shared-mq-context storm-id supervisor-id port worker-id]
  (log-message "Launching worker for " storm-id " on " supervisor-id ":" port " with id " worker-id
               " and conf " conf)
  (if-not (local-mode? conf)
    (redirect-stdio-to-log4j!))
  ;; because in local mode, its not a separate
  ;; process. supervisor will register it in this case
  (when (= :distributed (cluster-mode conf))
    (touch (worker-pid-path conf worker-id (process-pid))))
  (let [worker (worker-data conf shared-mq-context storm-id supervisor-id port worker-id)
        heartbeat-fn #(do-heartbeat worker)
        ;; do this here so that the worker process dies if this fails
        ;; it's important that worker heartbeat to supervisor ASAP when launching so that the supervisor knows it's running (and can move on)
        _ (heartbeat-fn)
        
        ;; heartbeat immediately to nimbus so that it knows that the worker has been started
        _ (do-executor-heartbeats worker)
        
        refresh-connections (mk-refresh-connections worker)

        _ (refresh-connections nil)
        _ (refresh-storm-active worker nil)
 
        executors (dofor [e (:executors worker)] (executor/mk-executor worker e))
        receive-thread-shutdown (launch-receive-thread worker)
        
        transfer-tuples (mk-transfer-tuples-handler worker)
                                               
        shutdown* (fn []
                    (log-message "Shutting down worker " storm-id " " supervisor-id " " port)
                    (doseq [[_ q] (:executor-receive-queue-map worker)]
                      (.haltProcessing q))
                    (doseq [executor executors] (.shutdown executor))
                    (doseq [[_ socket] @(:node+port->socket worker)]
                      ;; this will do best effort flushing since the linger period
                      ;; was set on creation
                      (.close socket))
                    (receive-thread-shutdown)
                    (log-message "Terminating zmq context")
                    
                    ;; now it's safe to shutdown receive queues (nothing left to add messages to it
                    ;; get deadlocked)
                    (doseq [[_ q] (:executor-receive-queue-map worker)]
                      (.shutdown q))
                    
                    ;;this is fine because the only time this is shared is when it's a local context,
                    ;;in which case it's a noop
                    (msg/term (:mq-context worker))
                    (log-message "Waiting for transfer queue to die")
                    (.shutdown (:transfer-queue worker))
                    (cancel-timer (:timer worker))
                    (.remove-worker-heartbeat! (:storm-cluster-state worker) storm-id supervisor-id port)
                    (log-message "Disconnecting from storm cluster state context")
                    (.disconnect (:storm-cluster-state worker))
                    (.close (:cluster-state worker))
                    (log-message "Shut down worker " storm-id " " supervisor-id " " port))
        ret (reify
             Shutdownable
             (shutdown
              [this]
              (shutdown*))
             DaemonCommon
             (waiting? [this]
                       (timer-waiting? (:timer worker)))
             )]
    (disruptor/set-handler (:transfer-queue worker) transfer-tuples)
    
    (schedule-recurring (:timer worker) 0 (conf TASK-REFRESH-POLL-SECS) refresh-connections)
    (schedule-recurring (:timer worker) 0 (conf TASK-REFRESH-POLL-SECS) (partial refresh-storm-active worker))
    (schedule-recurring (:timer worker) 0 (conf WORKER-HEARTBEAT-FREQUENCY-SECS) heartbeat-fn)
    (schedule-recurring (:timer worker) 0 (conf TASK-HEARTBEAT-FREQUENCY-SECS) #(do-executor-heartbeats worker :executors executors))

    (log-message "Worker has topology config " (:storm-conf worker))
    (log-message "Worker " worker-id " for storm " storm-id " on " supervisor-id ":" port " has finished loading")
    ret
    ))

(defmethod mk-suicide-fn
  :local [conf]
  (fn [] (halt-process! 1 "Worker died")))

(defmethod mk-suicide-fn
  :distributed [conf]
  (fn [] (halt-process! 1 "Worker died")))

(defn -main [storm-id supervisor-id port-str worker-id]  
  (let [conf (read-storm-config)]
    (validate-distributed-mode! conf)
    (mk-worker conf nil (java.net.URLDecoder/decode storm-id) supervisor-id (Integer/parseInt port-str) worker-id)))
