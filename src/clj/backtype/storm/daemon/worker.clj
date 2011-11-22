(ns backtype.storm.daemon.worker
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap])
  (:import [java.util.concurrent LinkedBlockingQueue])
  (:require [backtype.storm.daemon [task :as task]])
  (:gen-class))

(bootstrap)

(defmulti mk-suicide-fn cluster-mode)

(defn local-mode-zmq? [conf]
  (or (= (conf STORM-CLUSTER-MODE) "distributed")
      (conf STORM-LOCAL-MODE-ZMQ)))


(defn read-worker-task-ids [storm-cluster-state storm-id supervisor-id port]
  (let [assignment (:task->node+port (.assignment-info storm-cluster-state storm-id nil))]
    (doall
      (mapcat (fn [[task-id loc]]
              (if (= loc [supervisor-id port])
                [task-id]
                ))
            assignment))
    ))

(defn- read-storm-cache [conf storm-id]
  (let [stormroot (supervisor-stormdist-root conf storm-id)
        conf-path (supervisor-stormconf-path stormroot)
        topology-path (supervisor-stormcode-path stormroot)]
    [(merge conf (Utils/deserialize (FileUtils/readFileToByteArray (File. conf-path))))
     (Utils/deserialize (FileUtils/readFileToByteArray (File. topology-path)))]
    ))

(defn do-heartbeat [conf worker-id port storm-id task-ids]
  (.put (worker-state conf worker-id)
        LS-WORKER-HEARTBEAT
        (WorkerHeartbeat.
          (current-time-secs)
          storm-id
          task-ids
          port)))

(defn worker-outbound-tasks
  "Returns seq of task-ids that receive messages from this worker"
  ;; if this is an acker, needs to talk to the spouts
  [task->component mk-topology-context task-ids]
  (let [topology-context (mk-topology-context nil)
        spout-components (-> topology-context
                             .getRawTopology
                             .get_spouts
                             keys)
        contains-acker? (some? (fn [tid]
                                 (= ACKER-COMPONENT-ID
                                    (.getComponentId topology-context tid)))
                               task-ids)
        components (concat
                    (if contains-acker? spout-components)
                    (mapcat
                     (fn [task-id]
                       (let [context (mk-topology-context task-id)]
                         (->> (.getThisTargets context)
                              vals
                              (map keys)
                              (apply concat))
                         ))
                     task-ids))]
    (set
     (apply concat
            ;; fix this
            (-> (reverse-map task->component) (select-keys components) vals)))
    ))

;; TODO: should worker even take the storm-id as input? this should be
;; deducable from cluster state (by searching through assignments)
;; what about if there's inconsistency in assignments? -> but nimbus
;; should guarantee this consistency
;; TODO: consider doing worker heartbeating rather than task heartbeating to reduce the load on zookeeper
(defserverfn mk-worker [conf mq-context storm-id supervisor-id port worker-id]
  (log-message "Launching worker for " storm-id " on " supervisor-id ":" port " with id " worker-id)
  (let [active (atom true)
        storm-active-atom (atom false)
        cluster-state (cluster/mk-distributed-cluster-state conf)
        storm-cluster-state (cluster/mk-storm-cluster-state cluster-state)
        task-ids (read-worker-task-ids storm-cluster-state storm-id supervisor-id port)
        ;; because in local mode, its not a separate
        ;; process. supervisor will register it in this case
        _ (when (= :distributed (cluster-mode conf))
            (touch (worker-pid-path conf worker-id (process-pid))))
        heartbeat-fn #(do-heartbeat conf worker-id port storm-id task-ids)
        ;; do this here so that the worker process dies if this fails
        ;; it's important that worker heartbeat to supervisor ASAP when launching so that the supervisor knows it's running (and can move on)
        _ (heartbeat-fn)
        [storm-conf topology] (read-storm-cache conf storm-id)        
        event-manager (event/event-manager true)
        
        task->component (storm-task-info storm-cluster-state storm-id)
        mk-topology-context #(TopologyContext. (system-topology topology)
                                               task->component
                                               storm-id
                                               (supervisor-storm-resources-path
                                                 (supervisor-stormdist-root conf storm-id))
                                               (worker-pids-root conf worker-id)
                                               %)
        mk-user-context #(TopologyContext. topology
                                           task->component
                                           storm-id
                                           (supervisor-storm-resources-path
                                             (supervisor-stormdist-root conf storm-id))
                                           (worker-pids-root conf worker-id)
                                           %)
        mq-context (if mq-context
                     mq-context
                     (msg-loader/mk-zmq-context (storm-conf ZMQ-THREADS)
                                                (storm-conf ZMQ-LINGER-MILLIS)
                                                (= (conf STORM-CLUSTER-MODE) "local")))
        outbound-tasks (worker-outbound-tasks task->component mk-topology-context task-ids)
        endpoint-socket-lock (mk-rw-lock)
        node+port->socket (atom {})
        task->node+port (atom {})

        transfer-queue (LinkedBlockingQueue.) ; possibly bound the size of it
        
        transfer-fn (fn [task ^Tuple tuple]
                      (.put transfer-queue [task tuple])
                      )
        refresh-connections (fn this
                              ([]
                                (this (fn [& ignored] (.add event-manager this))))
                              ([callback]
                                (let [assignment (.assignment-info storm-cluster-state storm-id callback)
                                      my-assignment (select-keys (:task->node+port assignment) outbound-tasks)
                                      needed-connections (set (vals my-assignment))
                                      current-connections (set (keys @node+port->socket))
                                      new-connections (set/difference needed-connections current-connections)
                                      remove-connections (set/difference current-connections needed-connections)]
                                      (swap! node+port->socket
                                             merge
                                             (into {}
                                               (dofor [[node port :as endpoint] new-connections]
                                                 [endpoint
                                                  (msg/connect
                                                   mq-context
                                                   ((:node->host assignment) node)
                                                   port)
                                                  ]
                                                 )))
                                      (write-locked endpoint-socket-lock
                                        (reset! task->node+port my-assignment))
                                      (doseq [endpoint remove-connections]
                                        (.close (@node+port->socket endpoint)))
                                      (apply swap!
                                             node+port->socket
                                             dissoc
                                             remove-connections)
                                  )))

        refresh-storm-active (fn this
                               ([]
                                  (this (fn [& ignored] (.add event-manager this))))
                               ([callback]
                                 (let [base (.storm-base storm-cluster-state storm-id callback)]
                                  (reset!
                                   storm-active-atom
                                   (= :active (-> base :status :type))
                                   ))
                                  ))      
        _ (refresh-connections nil)
        _ (refresh-storm-active nil)

        heartbeat-thread (async-loop
                          (fn []
                            ;; this @active check handles the case where it's started after shutdown* joins to the thread
                            ;; if the thread is started after the join, then @active must be false. So there's no risk 
                            ;; of writing heartbeat after it's been shut down.
                            (when @active (heartbeat-fn) (conf WORKER-HEARTBEAT-FREQUENCY-SECS))
                            )
                          :priority Thread/MAX_PRIORITY)
        suicide-fn (mk-suicide-fn conf active)
        tasks (dofor [tid task-ids] (task/mk-task conf storm-conf (mk-topology-context tid) (mk-user-context tid) storm-id mq-context cluster-state storm-active-atom transfer-fn suicide-fn))
        threads [(async-loop
                  (fn []
                    (.add event-manager refresh-connections)
                    (.add event-manager refresh-storm-active)
                    (when @active (storm-conf TASK-REFRESH-POLL-SECS))
                    ))
                 (async-loop
                  (fn [^ArrayList drainer ^KryoTupleSerializer serializer]
                    (let [felem (.take transfer-queue)]
                      (.add drainer felem)
                      (.drainTo transfer-queue drainer))
                    (read-locked endpoint-socket-lock
                      (let [node+port->socket @node+port->socket
                            task->node+port @task->node+port]
                        (doseq [[task ^Tuple tuple] drainer]
                          (let [socket (node+port->socket (task->node+port task))
                                ser-tuple (.serialize serializer tuple)]
                            (msg/send socket task ser-tuple)
                            ))
                        ))
                    (.clear drainer)
                    0 )
                  :args-fn (fn [] [(ArrayList.) (KryoTupleSerializer. storm-conf (mk-topology-context nil))]))
                 heartbeat-thread]
        virtual-port-shutdown (when (local-mode-zmq? conf)
                                (log-message "Launching virtual port for " supervisor-id ":" port)
                                (msg-loader/launch-virtual-port!
                                 (= (conf STORM-CLUSTER-MODE) "local")
                                 mq-context
                                 port
                                 :kill-fn (fn [t]
                                            (halt-process! 11))
                                 :valid-ports task-ids))
        shutdown* (fn []
                    (log-message "Shutting down worker " storm-id " " supervisor-id " " port)
                    (reset! active false)
                    (doseq [task tasks] (.shutdown task))
                    (doseq [[_ socket] @node+port->socket]
                      ;; this will do best effort flushing since the linger period
                      ;; was set on creation
                      (.close socket))
                    (if virtual-port-shutdown (virtual-port-shutdown))
                    (log-message "Terminating zmq context")
                    (msg/term mq-context)
                    (log-message "Disconnecting from storm cluster state context")
                    (log-message "Waiting for heartbeat thread to die")
                    (doseq [t threads]
                      (.interrupt t)
                      (.join t))
                    (.shutdown event-manager)
                    (.disconnect storm-cluster-state)
                    (.close cluster-state)
                    (log-message "Shut down worker " storm-id " " supervisor-id " " port))
        ret (reify
             Shutdownable
             (shutdown
              [this]
              (shutdown*))
             DaemonCommon
             (waiting? [this]
                       (and
                        (.waiting? event-manager)
                        (every? (memfn waiting?) tasks)
                        (.sleeping? heartbeat-thread)))
             )]
    (log-message "Worker has topology config " storm-conf)
    (log-message "Worker " worker-id " for storm " storm-id " on " supervisor-id ":" port " has finished loading")
    ret
    ))

(defmethod mk-suicide-fn
  :local [conf active]
  (fn [] (reset! active false)))

(defmethod mk-suicide-fn
  :distributed [conf _]
  (fn [] (halt-process! 1 "Task died")))

(defn -main [storm-id supervisor-id port-str worker-id]  
  (let [conf (read-storm-config)]
    (validate-distributed-mode! conf)
    (mk-worker conf nil storm-id supervisor-id (Integer/parseInt port-str) worker-id)))
