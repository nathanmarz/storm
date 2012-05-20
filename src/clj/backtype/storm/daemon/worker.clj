(ns backtype.storm.daemon.worker
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap])
  (:import [java.util.concurrent LinkedBlockingQueue])
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

(defn mk-transfer-fn [worker]
  (let [receive-queue-map (:receive-queue-map worker)
        ^LinkedBlockingQueue transfer-queue (:transfer-queue worker)
        ^KryoTupleSerializer serializer (KryoTupleSerializer. (:storm-conf worker) (worker-context worker))]
    (fn [task ^Tuple tuple]
      (if (contains? receive-queue-map task)
        (.put ^LinkedBlockingQueue (receive-queue-map task) [task tuple])
        (let [tuple (.serialize serializer tuple)]
          (.put transfer-queue [task tuple]))
      ))))

(defn- mk-receive-queue-map [executors]
  (->> executors
       (map (fn [e] [e (LinkedBlockingQueue.)]))
       (mapcat (fn [[e queue]] (for [t (executor-id->tasks e)] [t queue])))
       (into {})
       ))

(defn worker-data [conf mq-context storm-id supervisor-id port worker-id]
  (let [cluster-state (cluster/mk-distributed-cluster-state conf)
        storm-cluster-state (cluster/mk-storm-cluster-state cluster-state)
        storm-conf (read-supervisor-storm-conf conf storm-id)
        executors (set (read-worker-executors storm-cluster-state storm-id supervisor-id port))
        transfer-queue (LinkedBlockingQueue.) ; possibly bound the size of it
        receive-queue-map (mk-receive-queue-map executors)
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
      :task-ids (keys receive-queue-map)
      :storm-conf storm-conf
      :topology topology
      :timer (mk-timer :kill-fn (fn [t]
                                  (log-error t "Error when processing event")
                                  (halt-process! 20 "Error when processing an event")
                                  ))
      :task->component (storm-task-info topology storm-conf)
      :endpoint-socket-lock (mk-rw-lock)
      :node+port->socket (atom {})
      :task->node+port (atom {})
      :transfer-queue transfer-queue
      :receive-queue-map receive-queue-map
      :suicide-fn (mk-suicide-fn conf)
      :uptime (uptime-computer)
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

(defn transfer-tuples [worker ^ArrayList drainer]
  (let [^LinkedBlockingQueue transfer-queue (:transfer-queue worker)
        felem (.take transfer-queue)]
    (.add drainer felem)
    (.drainTo transfer-queue drainer))
  (read-locked (:endpoint-socket-lock worker)
    (let [node+port->socket @(:node+port->socket worker)
          task->node+port @(:task->node+port worker)]
      ;; consider doing some automatic batching here (would need to not be serialized at this point to remove per-tuple overhead)
      (doseq [[task ser-tuple] drainer]
        (let [socket (node+port->socket (task->node+port task))]
          (msg/send socket task ser-tuple))
      )))
  (.clear drainer))

(defn launch-receive-thread [worker]
  (log-message "Launching receive-thread for " (:supervisor-id worker) ":" (:port worker))
  (msg-loader/launch-receive-thread!
    (:mq-context worker)
    (:storm-id worker)
    (:port worker)
    (:receive-queue-map worker)
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
        threads [(async-loop (fn [& args] (apply transfer-tuples args) 0)
                             :args-fn (fn [] [worker (ArrayList.)]))]
        receive-thread-shutdown (launch-receive-thread worker)
                                                              
        shutdown* (fn []
                    (log-message "Shutting down worker " storm-id " " supervisor-id " " port)
                    (doseq [executor executors] (.shutdown executor))
                    (doseq [[_ socket] @(:node+port->socket worker)]
                      ;; this will do best effort flushing since the linger period
                      ;; was set on creation
                      (.close socket))
                    (receive-thread-shutdown)
                    (log-message "Terminating zmq context")
                    ;;this is fine because the only time this is shared is when it's a local context,
                    ;;in which case it's a noop
                    (msg/term (:mq-context worker))
                    (log-message "Waiting for threads to die")
                    (doseq [t threads]
                      (.interrupt t)
                      (.join t))
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
                       (and
                        (timer-waiting? (:timer worker))))
             )]
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
