(ns backtype.storm.daemon.supervisor
  (:use [backtype.storm bootstrap])
  (:use [backtype.storm.daemon common])
  (:require [backtype.storm.daemon [worker :as worker]])
  (:gen-class))

(bootstrap)

(defmulti download-storm-code cluster-mode)
(defmulti launch-worker cluster-mode)

;; used as part of a map from port to this
(defrecord LocalAssignment [storm-id task-ids])

(defprotocol SupervisorDaemon
  (get-id [this])
  (get-conf [this])
  (shutdown-all-workers [this])
  )


(defn- read-my-tasks [storm-cluster-state storm-id supervisor-id callback]
  (let [assignment (.assignment-info storm-cluster-state storm-id callback)
        my-tasks (filter (fn [[_ [node _]]] (= node supervisor-id))
                         (:task->node+port assignment))
        port-tasks (apply merge-with
                          concat
                          (for [[task-id [_ port]] my-tasks]
                            {port [task-id]}
                            ))]
    (into {} (for [[port task-ids] port-tasks]
               ;; need to cast to int b/c it might be a long (due to how yaml parses things)
               [(int port) (LocalAssignment. storm-id task-ids)]
               ))
    ))

(defn- read-assignments
  "Returns map from port to struct containing :storm-id and :task-ids and :master-code-dir"
  [storm-cluster-state supervisor-id callback]
  (let [storm-ids (.assignments storm-cluster-state callback)]
    (apply merge-with
           (fn [& ignored]
             (throw (RuntimeException.
                     "Should not have multiple storms assigned to one port")))
           (dofor [sid storm-ids] (read-my-tasks storm-cluster-state sid supervisor-id callback))
           )))

(defn- read-storm-code-locations
  [storm-cluster-state callback]
  (let [storm-ids (.assignments storm-cluster-state callback)]
    (into {}
      (dofor [sid storm-ids]
        [sid (:master-code-dir (.assignment-info storm-cluster-state sid callback))]
        ))
    ))


(defn- read-downloaded-storm-ids [conf]
  (read-dir-contents (supervisor-stormdist-root conf))
  )

(defn read-worker-heartbeat [conf id]
  (let [local-state (worker-state conf id)]
    (.get local-state LS-WORKER-HEARTBEAT)
    ))


(defn my-worker-ids [conf]
  (read-dir-contents (worker-root conf)))

(defn read-worker-heartbeats
  "Returns map from worker id to heartbeat"
  [conf]
  (let [ids (my-worker-ids conf)]
    (into {}
      (dofor [id ids]
        [id (read-worker-heartbeat conf id)]))
    ))


(defn matches-an-assignment? [worker-heartbeat assigned-tasks]
  (let [local-assignment (assigned-tasks (:port worker-heartbeat))]
    (and local-assignment
         (= (:storm-id worker-heartbeat) (:storm-id local-assignment))
         (= (set (:task-ids worker-heartbeat)) (set (:task-ids local-assignment))))
    ))

(defn read-allocated-workers
  "Returns map from worker id to worker heartbeat. if the heartbeat is nil, then the worker is dead (timed out or never wrote heartbeat)"
  [conf local-state assigned-tasks]
  (let [now (current-time-secs)
        id->heartbeat (read-worker-heartbeats conf)
        approved-ids (set (keys (.get local-state LS-APPROVED-WORKERS)))]
    (into
     {}
     (dofor [[id hb] id->heartbeat]
            (let [state (cond
                         (or (not (contains? approved-ids id))
                             (not (matches-an-assignment? hb assigned-tasks)))
                           :disallowed
                         (not hb)
                           :not-started
                         (> (- now (:time-secs hb))
                            (conf SUPERVISOR-WORKER-TIMEOUT-SECS))
                           :timed-out
                         true
                           :valid)]
              (log-debug "Worker " id " is " state ": " hb)
              [id [state hb]]
              ))
     )))

(defn- wait-for-worker-launch [conf id start-time]
  (let [state (worker-state conf id)]    
    (loop []
      (let [hb (.get state LS-WORKER-HEARTBEAT)]
        (when (and
               (not hb)
               (<
                (- (current-time-secs) start-time)
                (conf SUPERVISOR-WORKER-START-TIMEOUT-SECS)
                ))
          (log-message id " still hasn't started")
          (Time/sleep 500)
          (recur)
          )))
    (when-not (.get state LS-WORKER-HEARTBEAT)
      (log-message "Worker " id " failed to start")
      )))

(defn- wait-for-workers-launch [conf ids]
  (let [start-time (current-time-secs)]
    (doseq [id ids]
      (wait-for-worker-launch conf id start-time))
    ))

(defn generate-supervisor-id []
  (uuid))

(defn try-cleanup-worker [conf id]
  (try
    (rmr (worker-heartbeats-root conf id))
    ;; this avoids a race condition with worker or subprocess writing pid around same time
    (rmpath (worker-pids-root conf id))
    (rmpath (worker-root conf id))
  (catch RuntimeException e
    (log-error e "Failed to cleanup worker " id ". Will retry later")
    )))

(defn shutdown-worker [conf supervisor-id id worker-thread-pids-atom]
  (log-message "Shutting down " supervisor-id ":" id)
  (let [pids (read-dir-contents (worker-pids-root conf id))
        thread-pid (@worker-thread-pids-atom id)]
    (when thread-pid
      (psim/kill-process thread-pid))
    (doseq [pid pids]
      (ensure-process-killed! pid)
      (rmpath (worker-pid-path conf id pid))
      )
    (try-cleanup-worker conf id))
  (log-message "Shut down " supervisor-id ":" id))

;; in local state, supervisor stores who its current assignments are
;; another thread launches events to restart any dead processes if necessary
(defserverfn mk-supervisor [conf shared-context]
  (FileUtils/cleanDirectory (File. (supervisor-tmp-dir conf)))
  (let [active (atom true)
        uptime (uptime-computer)
        worker-thread-pids-atom (atom {})
        storm-cluster-state (cluster/mk-storm-cluster-state conf)
        local-state (supervisor-state conf)
        my-hostname (local-hostname)
        supervisor-id (if-let [id (.get local-state LS-ID)] id (generate-supervisor-id))
        _ (.put local-state LS-ID supervisor-id)
        [event-manager processes-event-manager :as managers] [(event/event-manager false) (event/event-manager false)]
        sync-processes (fn []
                         (let [assigned-tasks (defaulted (.get local-state LS-LOCAL-ASSIGNMENTS) {})
                               allocated (read-allocated-workers conf local-state assigned-tasks)
                               keepers (filter-map-val
                                        (fn [[state _]] (= state :valid))
                                        allocated)
                               keep-ports (set (for [[id [_ hb]] keepers] (:port hb)))
                               reassign-tasks (select-keys-pred (complement keep-ports) assigned-tasks)
                               new-worker-ids (into
                                               {}
                                               (for [port (keys reassign-tasks)]
                                                 [port (uuid)]))
                               ]
                           ;; 1. to kill are those in allocated that are dead or disallowed
                           ;; 2. kill the ones that should be dead
                           ;;     - read pids, kill -9 and individually remove file
                           ;;     - rmr heartbeat dir, rmdir pid dir, rmdir id dir (catch exception and log)
                           ;; 3. of the rest, figure out what assignments aren't yet satisfied
                           ;; 4. generate new worker ids, write new "approved workers" to LS
                           ;; 5. create local dir for worker id
                           ;; 5. launch new workers (give worker-id, port, and supervisor-id)
                           ;; 6. wait for workers launch
                           
                           (log-debug "Syncing processes")
                           (log-debug "Assigned tasks: " assigned-tasks)
                           (log-debug "Allocated: " allocated)
                           (doseq [[id [state heartbeat]] allocated]
                             (when (not= :valid state)
                               (log-message
                                "Shutting down and clearing state for id " id
                                ". State: " state
                                ", Heartbeat: " (pr-str heartbeat))
                               (shutdown-worker conf supervisor-id id worker-thread-pids-atom)
                               ))
                           (doseq [id (vals new-worker-ids)]
                             (local-mkdirs (worker-pids-root conf id)))
                           (.put local-state LS-APPROVED-WORKERS
                                 (merge
                                  (select-keys (.get local-state LS-APPROVED-WORKERS)
                                               (keys keepers))
                                  (zipmap (vals new-worker-ids) (keys new-worker-ids))
                                  ))
                           (wait-for-workers-launch
                            conf
                            (dofor [[port assignment] reassign-tasks]
                              (let [id (new-worker-ids port)]
                                (log-message "Launching worker with assignment "
                                             (pr-str assignment)
                                             " for this supervisor "
                                             supervisor-id
                                             " on port "
                                             port
                                             " with id "
                                             id
                                             )
                                (launch-worker conf
                                               shared-context
                                               (:storm-id assignment)
                                               supervisor-id
                                               port
                                               id
                                               worker-thread-pids-atom)
                                id)))
                           ))
        synchronize-supervisor (fn this []
                                 (let [sync-callback (fn [& ignored] (.add event-manager this))
                                       storm-code-map (read-storm-code-locations storm-cluster-state sync-callback)
                                       assigned-storm-ids (set (keys storm-code-map))
                                       downloaded-storm-ids (set (read-downloaded-storm-ids conf))
                                       new-assignment (read-assignments
                                                        storm-cluster-state
                                                        supervisor-id
                                                        sync-callback)]
                                   (log-debug "Synchronizing supervisor")
                                   (log-debug "Storm code map: " storm-code-map)
                                   (log-debug "Downloaded storm ids: " downloaded-storm-ids)
                                   (log-debug "New assignment: " new-assignment)
                                   ;; download code first
                                   ;; This might take awhile
                                   ;;   - should this be done separately from usual monitoring?
                                   ;; should we only download when storm is assigned to this supervisor?
                                   (doseq [[storm-id master-code-dir] storm-code-map]
                                     (when-not (downloaded-storm-ids storm-id)
                                       (log-message "Downloading code for storm id "
                                          storm-id
                                          " from "
                                          master-code-dir)
                                       (download-storm-code conf storm-id master-code-dir)
                                       (log-message "Finished downloading code for storm id "
                                          storm-id
                                          " from "
                                          master-code-dir)
                                       ))
                                   ;; remove any downloaded code that's no longer assigned or active
                                   (doseq [storm-id downloaded-storm-ids]
                                     (when-not (assigned-storm-ids storm-id)
                                       (log-message "Removing code for storm id "
                                                    storm-id)
                                       (rmr (supervisor-stormdist-root conf storm-id))
                                       ))
                                   (log-debug "Writing new assignment "
                                              (pr-str new-assignment))
                                   (.put local-state
                                         LS-LOCAL-ASSIGNMENTS
                                         new-assignment
                                         )
                                   (.add processes-event-manager sync-processes)
                                   ))
        heartbeat-fn (fn [] (.supervisor-heartbeat!
                               storm-cluster-state
                               supervisor-id
                               (SupervisorInfo. (current-time-secs)
                                                my-hostname
                                                (conf SUPERVISOR-SLOTS-PORTS)
                                                (uptime))))
        _ (heartbeat-fn)
        ;; should synchronize supervisor so it doesn't launch anything after being down (optimization)
        threads (concat
                  [(async-loop
                     (fn []
                       (heartbeat-fn)
                       (when @active (conf SUPERVISOR-HEARTBEAT-FREQUENCY-SECS))
                       )
                     :priority Thread/MAX_PRIORITY)]
                   ;; This isn't strictly necessary, but it doesn't hurt and ensures that the machine stays up
                   ;; to date even if callbacks don't all work exactly right
                   (when (conf SUPERVISOR-ENABLE)
                     [(async-loop
                       (fn []
                         (.add event-manager synchronize-supervisor)
                         (when @active 10)
                         ))
                      (async-loop
                         (fn []
                           (.add processes-event-manager sync-processes)
                           (when @active (conf SUPERVISOR-MONITOR-FREQUENCY-SECS))
                           )
                         :priority Thread/MAX_PRIORITY)]))]
    (log-message "Starting supervisor with id " supervisor-id)
    (reify
     Shutdownable
     (shutdown [this]
               (log-message "Shutting down supervisor " supervisor-id)
               (reset! active false)
               (doseq [t threads]
                 (.interrupt t)
                 (.join t))
               (.shutdown event-manager)
               (.shutdown processes-event-manager)
               (.disconnect storm-cluster-state))
     SupervisorDaemon
     (get-conf [this]
       conf)
     (get-id [this]
       supervisor-id )
     (shutdown-all-workers [this]
       (let [ids (my-worker-ids conf)]
         (doseq [id ids]
           (shutdown-worker conf supervisor-id id worker-thread-pids-atom)
           )))
     DaemonCommon
     (waiting? [this]
       (or (not @active)
           (and
            (every? (memfn sleeping?) threads)
            (every? (memfn waiting?) managers)))
           ))))

(defn kill-supervisor [supervisor]
  (.shutdown supervisor)
  )

;; distributed implementation

(defmethod download-storm-code
    :distributed [conf storm-id master-code-dir]
    ;; Downloading to permanent location is atomic
    (let [tmproot (str (supervisor-tmp-dir conf) "/" (uuid))
          stormroot (supervisor-stormdist-root conf storm-id)]
      (FileUtils/forceMkdir (File. tmproot))      
      
      (Utils/downloadFromMaster conf (master-stormjar-path master-code-dir) (supervisor-stormjar-path tmproot))
      (Utils/downloadFromMaster conf (master-stormcode-path master-code-dir) (supervisor-stormcode-path tmproot))
      (Utils/downloadFromMaster conf (master-stormconf-path master-code-dir) (supervisor-stormconf-path tmproot))
      (extract-dir-from-jar (supervisor-stormjar-path tmproot) RESOURCES-SUBDIR tmproot)
      (FileUtils/moveDirectory (File. tmproot) (File. stormroot))
      ))


(defmethod launch-worker
    :distributed [conf shared-context storm-id supervisor-id port worker-id worker-thread-pids-atom]
    (let [stormroot (supervisor-stormdist-root conf storm-id)
          stormjar (supervisor-stormjar-path stormroot)
          classpath (add-to-classpath (current-classpath) [stormjar])
          childopts (.replaceAll (conf WORKER-CHILDOPTS) "%ID%" (str port))
          logfilename (str "worker-" port ".log")
          command (str "java -server " childopts
                       " -Djava.library.path=" (conf JAVA-LIBRARY-PATH)
                       " -Dlogfile.name=" logfilename
                       " -Dlog4j.configuration=storm.log.properties"
                       " -cp " classpath " backtype.storm.daemon.worker "
                       storm-id " " supervisor-id " " port " " worker-id)]
      (log-message "Launching worker with command: " command)
      (launch-process command)
      ))

;; local implementation

(defmethod download-storm-code
    :local [conf storm-id master-code-dir]
  (let [stormroot (supervisor-stormdist-root conf storm-id)]
      (FileUtils/copyDirectory (File. master-code-dir) (File. stormroot))
      (let [classloader (.getContextClassLoader (Thread/currentThread))
            ;; should detect if it was run with "storm jar" and copy or extract appropriately
            url (.getResource classloader RESOURCES-SUBDIR)
            target-dir (str stormroot "/" RESOURCES-SUBDIR)]
            (when url
              (log-message "Copying resources at " (str url) " to " target-dir)
              (FileUtils/copyDirectory (File. (.getFile url)) (File. target-dir))
              ))))

(defmethod launch-worker
    :local [conf shared-context storm-id supervisor-id port worker-id worker-thread-pids-atom]
    (let [pid (uuid)
          worker (worker/mk-worker conf shared-context storm-id supervisor-id port worker-id)]
      (psim/register-process pid worker)
      (swap! worker-thread-pids-atom assoc worker-id pid)
      ))

(defn -main []
  (let [conf (read-storm-config)]
    (validate-distributed-mode! conf)
    (mk-supervisor conf nil)))
