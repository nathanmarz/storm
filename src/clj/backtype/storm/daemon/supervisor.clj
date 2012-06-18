(ns backtype.storm.daemon.supervisor
  (:import [backtype.storm.scheduler ISupervisor])
  (:use [backtype.storm bootstrap])
  (:use [backtype.storm.daemon common])
  (:require [backtype.storm.daemon [worker :as worker]])
  (:gen-class
    :methods [^{:static true} [launch [backtype.storm.scheduler.ISupervisor] void]]))

(bootstrap)

(defmulti download-storm-code cluster-mode)
(defmulti launch-worker (fn [supervisor & _] (cluster-mode (:conf supervisor))))

;; used as part of a map from port to this
(defrecord LocalAssignment [storm-id executors])

(defprotocol SupervisorDaemon
  (get-id [this])
  (get-conf [this])
  (shutdown-all-workers [this])
  )


(defn- read-my-executors [storm-cluster-state storm-id supervisor-id callback]
  (let [assignment (.assignment-info storm-cluster-state storm-id callback)
        my-executors (filter (fn [[_ [node _]]] (= node supervisor-id))
                               (:executor->node+port assignment))
        port-executors (apply merge-with
                          concat
                          (for [[executor [_ port]] my-executors]
                            {port [executor]}
                            ))]
    (into {} (for [[port executors] port-executors]
               ;; need to cast to int b/c it might be a long (due to how yaml parses things)
               [(Integer. port) (LocalAssignment. storm-id executors)]
               ))
    ))

(defn- read-assignments
  "Returns map from port to struct containing :storm-id and :executors and :master-code-dir"
  [storm-cluster-state supervisor-id callback]
  (let [storm-ids (.assignments storm-cluster-state callback)]
    (apply merge-with
           (fn [& ignored]
             (throw (RuntimeException.
                     "Should not have multiple topologies assigned to one port")))
           (dofor [sid storm-ids] (read-my-executors storm-cluster-state sid supervisor-id callback))
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
  (map #(java.net.URLDecoder/decode %) (read-dir-contents (supervisor-stormdist-root conf)))
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


(defn matches-an-assignment? [worker-heartbeat assigned-executors]
  (let [local-assignment (assigned-executors (:port worker-heartbeat))]
    (and local-assignment
         (= (:storm-id worker-heartbeat) (:storm-id local-assignment))
         (= (set (:executors worker-heartbeat)) (set (:executors local-assignment))))
    ))

(defn read-allocated-workers
  "Returns map from worker id to worker heartbeat. if the heartbeat is nil, then the worker is dead (timed out or never wrote heartbeat)"
  [supervisor assigned-executors]
  (let [conf (:conf supervisor)
        ^LocalState local-state (:local-state supervisor)
        now (current-time-secs)
        id->heartbeat (read-worker-heartbeats conf)
        approved-ids (set (keys (.get local-state LS-APPROVED-WORKERS)))]
    (into
     {}
     (dofor [[id hb] id->heartbeat]
            (let [state (cond
                         (or (not (contains? approved-ids id))
                             (not (matches-an-assignment? hb assigned-executors)))
                           :disallowed
                         (not hb)
                           :not-started
                         (> (- now (:time-secs hb))
                            (conf SUPERVISOR-WORKER-TIMEOUT-SECS))
                           :timed-out
                         true
                           :valid)]
              (log-debug "Worker " id " is " state ": " (pr-str hb) " at supervisor time-secs " now)
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
    (log-warn-error e "Failed to cleanup worker " id ". Will retry later")
    )))

(defn shutdown-worker [supervisor id]
  (log-message "Shutting down " (:supervisor-id supervisor) ":" id)
  (let [conf (:conf supervisor)
        pids (read-dir-contents (worker-pids-root conf id))
        thread-pid (@(:worker-thread-pids-atom supervisor) id)]
    (when thread-pid
      (psim/kill-process thread-pid))
    (doseq [pid pids]
      (ensure-process-killed! pid)
      (rmpath (worker-pid-path conf id pid))
      )
    (try-cleanup-worker conf id))
  (log-message "Shut down " (:supervisor-id supervisor) ":" id))

(defn supervisor-data [conf shared-context ^ISupervisor isupervisor]
  {:conf conf
   :shared-context shared-context
   :isupervisor isupervisor
   :active (atom true)
   :uptime (uptime-computer)
   :worker-thread-pids-atom (atom {})
   :storm-cluster-state (cluster/mk-storm-cluster-state conf)
   :local-state (supervisor-state conf)
   :supervisor-id (.getId isupervisor)
   :my-hostname (if (contains? conf STORM-LOCAL-HOSTNAME)
                  (conf STORM-LOCAL-HOSTNAME)
                  (local-hostname))
   :timer (mk-timer :kill-fn (fn [t]
                               (log-error t "Error when processing event")
                               (halt-process! 20 "Error when processing an event")
                               ))
   })

(defn sync-processes [supervisor]
  (let [conf (:conf supervisor)
        ^LocalState local-state (:local-state supervisor)
        assigned-executors (defaulted (.get local-state LS-LOCAL-ASSIGNMENTS) {})
        allocated (read-allocated-workers supervisor assigned-executors)
        keepers (filter-val
                 (fn [[state _]] (= state :valid))
                 allocated)
        keep-ports (set (for [[id [_ hb]] keepers] (:port hb)))
        reassign-executors (select-keys-pred (complement keep-ports) assigned-executors)
        new-worker-ids (into
                        {}
                        (for [port (keys reassign-executors)]
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
    (log-debug "Assigned executors: " assigned-executors)
    (log-debug "Allocated: " allocated)
    (doseq [[id [state heartbeat]] allocated]
      (when (not= :valid state)
        (log-message
         "Shutting down and clearing state for id " id
         ". State: " state
         ", Heartbeat: " (pr-str heartbeat))
        (shutdown-worker supervisor id)
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
     (dofor [[port assignment] reassign-executors]
       (let [id (new-worker-ids port)]
         (log-message "Launching worker with assignment "
                      (pr-str assignment)
                      " for this supervisor "
                      (:supervisor-id supervisor)
                      " on port "
                      port
                      " with id "
                      id
                      )
         (launch-worker supervisor
                        (:storm-id assignment)
                        port
                        id)
         id)))
    ))

(defn mk-synchronize-supervisor [supervisor sync-processes event-manager processes-event-manager]
  (fn this []
    (let [conf (:conf supervisor)
          storm-cluster-state (:storm-cluster-state supervisor)
          ^ISupervisor isupervisor (:isupervisor supervisor)
          ^LocalState local-state (:local-state supervisor)
          sync-callback (fn [& ignored] (.add event-manager this))
          storm-code-map (read-storm-code-locations storm-cluster-state sync-callback)
          assigned-storm-ids (set (keys storm-code-map))
          downloaded-storm-ids (set (read-downloaded-storm-ids conf))
          all-assignment (read-assignments
                           storm-cluster-state
                           (:supervisor-id supervisor)
                           sync-callback)
          new-assignment (->> all-assignment
                              (filter-key #(.confirmAssigned isupervisor %)))
          existing-assignment (.get local-state LS-LOCAL-ASSIGNMENTS)]
      (log-debug "Synchronizing supervisor")
      (log-debug "Storm code map: " storm-code-map)
      (log-debug "Downloaded storm ids: " downloaded-storm-ids)
      (log-debug "All assignment: " all-assignment)
      (log-debug "New assignment: " new-assignment)
      
      ;; download code first
      ;; This might take awhile
      ;;   - should this be done separately from usual monitoring?
      ;; should we only download when topology is assigned to this supervisor?
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
      (doseq [p (set/difference (set (keys existing-assignment))
                                (set (keys new-assignment)))]
        (.killedWorker isupervisor (int p)))
      (.put local-state
            LS-LOCAL-ASSIGNMENTS
            new-assignment)
      (.add processes-event-manager sync-processes)
      )))

;; in local state, supervisor stores who its current assignments are
;; another thread launches events to restart any dead processes if necessary
(defserverfn mk-supervisor [conf shared-context ^ISupervisor isupervisor]
  (log-message "Starting Supervisor with conf " conf)
  (.prepare isupervisor conf (supervisor-isupervisor-dir conf))
  (FileUtils/cleanDirectory (File. (supervisor-tmp-dir conf)))
  (let [supervisor (supervisor-data conf shared-context isupervisor)
        [event-manager processes-event-manager :as managers] [(event/event-manager false) (event/event-manager false)]                         
        sync-processes (partial sync-processes supervisor)
        synchronize-supervisor (mk-synchronize-supervisor supervisor sync-processes event-manager processes-event-manager)
        heartbeat-fn (fn [] (.supervisor-heartbeat!
                               (:storm-cluster-state supervisor)
                               (:supervisor-id supervisor)
                               (SupervisorInfo. (current-time-secs)
                                                (:my-hostname supervisor)
                                                (.getMetadata isupervisor)
                                                (conf SUPERVISOR-SCHEDULER-META)
                                                ((:uptime supervisor)))))]
    (heartbeat-fn)
    ;; should synchronize supervisor so it doesn't launch anything after being down (optimization)
    (schedule-recurring (:timer supervisor)
                        0
                        (conf SUPERVISOR-HEARTBEAT-FREQUENCY-SECS)
                        heartbeat-fn)
    (when (conf SUPERVISOR-ENABLE)
      ;; This isn't strictly necessary, but it doesn't hurt and ensures that the machine stays up
      ;; to date even if callbacks don't all work exactly right
      (schedule-recurring (:timer supervisor) 0 10 (fn [] (.add event-manager synchronize-supervisor)))
      (schedule-recurring (:timer supervisor)
                          0
                          (conf SUPERVISOR-MONITOR-FREQUENCY-SECS)
                          (fn [] (.add processes-event-manager sync-processes))))
    (log-message "Starting supervisor with id " (:supervisor-id supervisor) " at host " (:my-hostname supervisor))
    (reify
     Shutdownable
     (shutdown [this]
               (log-message "Shutting down supervisor " (:supervisor-id supervisor))
               (reset! (:active supervisor) false)
               (cancel-timer (:timer supervisor))
               (.shutdown event-manager)
               (.shutdown processes-event-manager)
               (.disconnect (:storm-cluster-state supervisor)))
     SupervisorDaemon
     (get-conf [this]
       conf)
     (get-id [this]
       (:supervisor-id supervisor))
     (shutdown-all-workers [this]
       (let [ids (my-worker-ids conf)]
         (doseq [id ids]
           (shutdown-worker supervisor id)
           )))
     DaemonCommon
     (waiting? [this]
       (or (not @(:active supervisor))
           (and
            (timer-waiting? (:timer supervisor))
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
    :distributed [supervisor storm-id port worker-id]
    (let [conf (:conf supervisor)
          stormroot (supervisor-stormdist-root conf storm-id)
          stormjar (supervisor-stormjar-path stormroot)
          storm-conf (read-supervisor-storm-conf conf storm-id)
          classpath (add-to-classpath (current-classpath) [stormjar])
          childopts (.replaceAll (str (conf WORKER-CHILDOPTS) " " (storm-conf TOPOLOGY-WORKER-CHILDOPTS))
                                 "%ID%"
                                 (str port))
          logfilename (str "worker-" port ".log")
          command (str "java -server " childopts
                       " -Djava.library.path=" (conf JAVA-LIBRARY-PATH)
                       " -Dlogfile.name=" logfilename
                       " -Dstorm.home=" (System/getProperty "storm.home")
                       " -Dlog4j.configuration=storm.log.properties"
                       " -cp " classpath " backtype.storm.daemon.worker "
                       (java.net.URLEncoder/encode storm-id) " " (:supervisor-id supervisor)
                       " " port " " worker-id)]
      (log-message "Launching worker with command: " command)
      (launch-process command :environment {"LD_LIBRARY_PATH" (conf JAVA-LIBRARY-PATH)})
      ))

;; local implementation

(defn resources-jar []
  (->> (.split (current-classpath) File/pathSeparator)
       (filter #(.endsWith  % ".jar"))
       (filter #(zip-contains-dir? % RESOURCES-SUBDIR))
       first ))

(defmethod download-storm-code
    :local [conf storm-id master-code-dir]
  (let [stormroot (supervisor-stormdist-root conf storm-id)]
      (FileUtils/copyDirectory (File. master-code-dir) (File. stormroot))
      (let [classloader (.getContextClassLoader (Thread/currentThread))
            resources-jar (resources-jar)
            url (.getResource classloader RESOURCES-SUBDIR)
            target-dir (str stormroot "/" RESOURCES-SUBDIR)]
            (cond
              resources-jar
              (do
                (log-message "Extracting resources from jar at " resources-jar " to " target-dir)
                (extract-dir-from-jar resources-jar RESOURCES-SUBDIR stormroot))
              url
              (do
                (log-message "Copying resources at " (str url) " to " target-dir)
                (FileUtils/copyDirectory (File. (.getFile url)) (File. target-dir))
                ))
            )))

(defmethod launch-worker
    :local [supervisor storm-id port worker-id]
    (let [conf (:conf supervisor)
          pid (uuid)
          worker (worker/mk-worker conf
                                   (:shared-context supervisor)
                                   storm-id
                                   (:supervisor-id supervisor)
                                   port
                                   worker-id)]
      (psim/register-process pid worker)
      (swap! (:worker-thread-pids-atom supervisor) assoc worker-id pid)
      ))

(defn -launch [supervisor]
  (let [conf (read-storm-config)]
    (validate-distributed-mode! conf)
    (mk-supervisor conf nil supervisor)))

(defn standalone-supervisor []
  (let [conf-atom (atom nil)
        id-atom (atom nil)]
    (reify ISupervisor
      (prepare [this conf local-dir]
        (reset! conf-atom conf)
        (let [state (LocalState. local-dir)
              curr-id (if-let [id (.get state LS-ID)]
                        id
                        (generate-supervisor-id))]
          (.put state LS-ID curr-id)
          (reset! id-atom curr-id))
        )
      (confirmAssigned [this port]
        true)
      (getMetadata [this]
        (get @conf-atom SUPERVISOR-SLOTS-PORTS))
      (getId [this]
        @id-atom)
      (killedWorker [this port]
        ))))

(defn -main []
  (-launch (standalone-supervisor)))
