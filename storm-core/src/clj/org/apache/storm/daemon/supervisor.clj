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
(ns org.apache.storm.daemon.supervisor
  (:import [java.io File IOException FileOutputStream])
  (:import [org.apache.storm.scheduler ISupervisor]
           [org.apache.storm.utils LocalState Time Utils Utils$ExitCodeCallable
                                   ConfigUtils]
           [org.apache.storm.daemon Shutdownable]
           [org.apache.storm Constants]
           [org.apache.storm.cluster ClusterStateContext DaemonType StormClusterStateImpl ClusterUtils IStateStorage]
           [java.net JarURLConnection]
           [java.net URI URLDecoder]
           [org.apache.commons.io FileUtils])
  (:use [org.apache.storm config util log converter local-state-converter])
  (:import [org.apache.storm.generated AuthorizationException KeyNotFoundException WorkerResources])
  (:import [org.apache.storm.utils NimbusLeaderNotFoundException VersionInfo])
  (:import [java.nio.file Files StandardCopyOption])
  (:import [org.apache.storm.generated WorkerResources ProfileAction LocalAssignment])
  (:import [org.apache.storm Config ProcessSimulator])
  (:import [org.apache.storm.localizer LocalResource])
  (:import [org.apache.storm.event EventManagerImp])
  (:use [org.apache.storm.daemon common])
  (:import [org.apache.storm.command HealthCheck])
  (:require [org.apache.storm.daemon [worker :as worker]]

            [clojure.set :as set])
  (:import [org.apache.thrift.transport TTransportException])
  (:import [org.apache.zookeeper data.ACL ZooDefs$Ids ZooDefs$Perms])
  (:import [org.yaml.snakeyaml Yaml]
           [org.yaml.snakeyaml.constructor SafeConstructor])
  (:require [metrics.gauges :refer [defgauge]])
  (:require [metrics.meters :refer [defmeter mark!]])
  (:import [org.apache.storm StormTimer])
  (:gen-class
    :methods [^{:static true} [launch [org.apache.storm.scheduler.ISupervisor] void]])
  (:require [clojure.string :as str]))

(defmeter supervisor:num-workers-launched)

(defmulti download-storm-code cluster-mode)
(defmulti launch-worker (fn [supervisor & _] (cluster-mode (:conf supervisor))))

(def STORM-VERSION (VersionInfo/getVersion))

(defprotocol SupervisorDaemon
  (get-id [this])
  (get-conf [this])
  (shutdown-all-workers [this])
  )

;TODO: when translating this function, you should replace the filter-val with a proper for loop + if condition HERE
(defn- assignments-snapshot [storm-cluster-state callback assignment-versions]
  (let [storm-ids (.assignments storm-cluster-state callback)]
    (let [new-assignments
          (->>
           (dofor [sid storm-ids]
                  (let [recorded-version (:version (get assignment-versions sid))]
                    (if-let [assignment-version (.assignmentVersion storm-cluster-state sid callback)]
                      (if (= assignment-version recorded-version)
                        {sid (get assignment-versions sid)}
                        (let [thriftify-assignment-version (.assignmentInfoWithVersion storm-cluster-state sid callback)
                              assignment (clojurify-assignment (.get thriftify-assignment-version (IStateStorage/DATA)))]
                        {sid {:data assignment :version (.get thriftify-assignment-version (IStateStorage/VERSION))}}))
                      {sid nil})))
           (apply merge)
           (filter-val not-nil?))
          new-profiler-actions
          (->>
            (dofor [sid (distinct storm-ids)]

                   (if-let [topo-profile-actions (into [] (for [request (.getTopologyProfileRequests storm-cluster-state sid)] (clojurify-profile-request request)))]
                      {sid topo-profile-actions}))
           (apply merge))]
      {:assignments (into {} (for [[k v] new-assignments] [k (:data v)]))
       :profiler-actions new-profiler-actions
       :versions new-assignments})))

(defn mk-local-assignment
  [storm-id executors resources]
  {:storm-id storm-id :executors executors :resources resources})

(defn- read-my-executors [assignments-snapshot storm-id assignment-id]
  (let [assignment (get assignments-snapshot storm-id)
        my-slots-resources (into {}
                                 (filter (fn [[[node _] _]] (= node assignment-id))
                                         (:worker->resources assignment)))
        my-executors (filter (fn [[_ [node _]]] (= node assignment-id))
                             (:executor->node+port assignment))
        port-executors (apply merge-with
                              concat
                              (for [[executor [_ port]] my-executors]
                                {port [executor]}
                                ))]
    (into {} (for [[port executors] port-executors]
               ;; need to cast to int b/c it might be a long (due to how yaml parses things)
               ;; doall is to avoid serialization/deserialization problems with lazy seqs
               [(Integer. port) (mk-local-assignment storm-id (doall executors) (get my-slots-resources [assignment-id port]))]
               ))))

(defn- read-assignments
  "Returns map from port to struct containing :storm-id, :executors and :resources"
  ([assignments-snapshot assignment-id]
     (->> (dofor [sid (keys assignments-snapshot)] (read-my-executors assignments-snapshot sid assignment-id))
          (apply merge-with (fn [& ignored] (throw (RuntimeException. (str "Should not have multiple topologies assigned to one port")))))))
  ([assignments-snapshot assignment-id existing-assignment retries]
     (try (let [assignments (read-assignments assignments-snapshot assignment-id)]
            (reset! retries 0)
            assignments)
          (catch RuntimeException e
            (if (> @retries 2) (throw e) (swap! retries inc))
            (log-warn (.getMessage e) ": retrying " @retries " of 3")
            existing-assignment))))

;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
(defn- read-storm-code-locations
  [assignments-snapshot]
  (map-val :master-code-dir assignments-snapshot))

(defn- read-downloaded-storm-ids [conf]
  (map #(URLDecoder/decode %) (Utils/readDirContents (ConfigUtils/supervisorStormDistRoot conf))))

(defn ->executor-list
  [executors]
  (into []
        (for [exec-info executors]
          [(.get_task_start exec-info) (.get_task_end exec-info)])))

(defn ls-worker-heartbeat
  [^LocalState local-state]
  (if-let [worker-hb (.getWorkerHeartBeat ^LocalState local-state)]
    {:time-secs (.get_time_secs worker-hb)
     :storm-id (.get_topology_id worker-hb)
     :executors (->executor-list (.get_executors worker-hb))
     :port (.get_port worker-hb)}))

(defn read-worker-heartbeat [conf id]
  (let [local-state (ConfigUtils/workerState conf id)]
    (try
      (ls-worker-heartbeat local-state)
      (catch Exception e
        (log-warn e "Failed to read local heartbeat for workerId : " id ",Ignoring exception.")
        nil))))


(defn my-worker-ids [conf]
  (Utils/readDirContents (ConfigUtils/workerRoot conf)))

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
         (= (disj (set (:executors worker-heartbeat)) Constants/SYSTEM_EXECUTOR_ID)
            (set (:executors local-assignment))))))

(let [dead-workers (atom #{})]
  (defn get-dead-workers []
    @dead-workers)
  (defn add-dead-worker [worker]
    (swap! dead-workers conj worker))
  (defn remove-dead-worker [worker]
    (swap! dead-workers disj worker)))

(defn is-worker-hb-timed-out? [now hb conf]
  (> (- now (:time-secs hb))
     (conf SUPERVISOR-WORKER-TIMEOUT-SECS)))

(defn read-allocated-workers
  "Returns map from worker id to worker heartbeat. if the heartbeat is nil, then the worker is dead (timed out or never wrote heartbeat)"
  [supervisor assigned-executors now]
  (let [conf (:conf supervisor)
        ^LocalState local-state (:local-state supervisor)
        id->heartbeat (read-worker-heartbeats conf)
        approved-ids (set (keys (clojurify-structure (.getApprovedWorkers ^LocalState local-state))))]
    (into
     {}
     (dofor [[id hb] id->heartbeat]
            (let [state (cond
                         (not hb)
                           :not-started
                         (or (not (contains? approved-ids id))
                             (not (matches-an-assignment? hb assigned-executors)))
                           :disallowed
                         (or
                          (when (get (get-dead-workers) id)
                            (log-message "Worker Process " id " has died!")
                            true)
                          (is-worker-hb-timed-out? now hb conf))
                           :timed-out
                         true
                           :valid)]
              (log-debug "Worker " id " is " state ": " (pr-str hb) " at supervisor time-secs " now)
              [id [state hb]]
              ))
     )))

(defn- wait-for-worker-launch [conf id start-time]
  (let [state (ConfigUtils/workerState conf id)]
    (loop []
      (let [hb (.getWorkerHeartBeat state)]
        (when (and
               (not hb)
               (<
                (- (Time/currentTimeSecs) start-time)
                (conf SUPERVISOR-WORKER-START-TIMEOUT-SECS)
                ))
          (log-message id " still hasn't started")
          (Time/sleep 500)
          (recur)
          )))
    (when-not (.getWorkerHeartBeat state)
      (log-message "Worker " id " failed to start")
      )))

(defn- wait-for-workers-launch [conf ids]
  (let [start-time (Time/currentTimeSecs)]
    (doseq [id ids]
      (wait-for-worker-launch conf id start-time))
    ))

(defn generate-supervisor-id []
  (Utils/uuid))

(defnk worker-launcher [conf user args :environment {} :log-prefix nil :exit-code-callback nil :directory nil]
  (let [_ (when (clojure.string/blank? user)
            (throw (java.lang.IllegalArgumentException.
                     "User cannot be blank when calling worker-launcher.")))
        wl-initial (conf SUPERVISOR-WORKER-LAUNCHER)
        storm-home (System/getProperty "storm.home")
        wl (if wl-initial wl-initial (str storm-home "/bin/worker-launcher"))
        command (concat [wl user] args)]
    (log-message "Running as user:" user " command:" (pr-str command))
    (Utils/launchProcess command
                         environment
                         log-prefix
                         exit-code-callback
                         directory)))

(defnk worker-launcher-and-wait [conf user args :environment {} :log-prefix nil]
  (let [process (worker-launcher conf user args :environment environment)]
    (if log-prefix
      (Utils/readAndLogStream log-prefix (.getInputStream process)))
      (try
        (.waitFor process)
      (catch InterruptedException e
        (log-message log-prefix " interrupted.")))
      (.exitValue process)))

(defn- rmr-as-user
  "Launches a process owned by the given user that deletes the given path
  recursively.  Throws RuntimeException if the directory is not removed."
  [conf id path]
  (let [user (Utils/getFileOwner path)]
    (worker-launcher-and-wait conf
      user
      ["rmr" path]
      :log-prefix (str "rmr " id))
    (if (Utils/checkFileExists path)
      (throw (RuntimeException. (str path " was not deleted"))))))

(defn try-cleanup-worker [conf supervisor id]
  (try
    (if (.exists (File. (ConfigUtils/workerRoot conf id)))
      (do
        (if (conf SUPERVISOR-RUN-WORKER-AS-USER)
          (rmr-as-user conf id (ConfigUtils/workerRoot conf id))
          (do
            (Utils/forceDelete (ConfigUtils/workerHeartbeatsRoot conf id))
            ;; this avoids a race condition with worker or subprocess writing pid around same time
            (Utils/forceDelete (ConfigUtils/workerPidsRoot conf id))
            (Utils/forceDelete (ConfigUtils/workerRoot conf id))))
        (ConfigUtils/removeWorkerUserWSE conf id)
        (remove-dead-worker id)
      ))
    (if (conf STORM-RESOURCE-ISOLATION-PLUGIN-ENABLE)
      (.releaseResourcesForWorker (:resource-isolation-manager supervisor) id))
  (catch IOException e
    (log-warn-error e "Failed to cleanup worker " id ". Will retry later"))
  (catch RuntimeException e
    (log-warn-error e "Failed to cleanup worker " id ". Will retry later")
    )
  (catch java.io.FileNotFoundException e (log-message (.getMessage e)))
    ))

(defn shutdown-worker [supervisor id]
  (log-message "Shutting down " (:supervisor-id supervisor) ":" id)
  (let [conf (:conf supervisor)
        pids (Utils/readDirContents (ConfigUtils/workerPidsRoot conf id))
        thread-pid (@(:worker-thread-pids-atom supervisor) id)
        shutdown-sleep-secs (conf SUPERVISOR-WORKER-SHUTDOWN-SLEEP-SECS)
        as-user (conf SUPERVISOR-RUN-WORKER-AS-USER)
        user (ConfigUtils/getWorkerUser conf id)]
    (when thread-pid
      (ProcessSimulator/killProcess thread-pid))
    (doseq [pid pids]
      (if as-user
        (worker-launcher-and-wait conf user ["signal" pid "15"] :log-prefix (str "kill -15 " pid))
        (Utils/killProcessWithSigTerm pid)))
    (when-not (empty? pids)  
      (log-message "Sleep " shutdown-sleep-secs " seconds for execution of cleanup threads on worker.")
      (Time/sleepSecs shutdown-sleep-secs))
    (doseq [pid pids]
      (if as-user
        (worker-launcher-and-wait conf user ["signal" pid "9"] :log-prefix (str "kill -9 " pid))
        (Utils/forceKillProcess pid))
      (let [path (ConfigUtils/workerPidPath conf id pid)]
        (if as-user
          (rmr-as-user conf id path)
          (try
            (log-debug "Removing path " path)
            (.delete (File. path))
            (catch Exception e))))) ;; on windows, the supervisor may still holds the lock on the worker directory
    (try-cleanup-worker conf supervisor id))
  (log-message "Shut down " (:supervisor-id supervisor) ":" id))

(def SUPERVISOR-ZK-ACLS
  [(first ZooDefs$Ids/CREATOR_ALL_ACL)
   (ACL. (bit-or ZooDefs$Perms/READ ZooDefs$Perms/CREATE) ZooDefs$Ids/ANYONE_ID_UNSAFE)])

(defn supervisor-data [conf shared-context ^ISupervisor isupervisor]
  {:conf conf
   :shared-context shared-context
   :isupervisor isupervisor
   :active (atom true)
   :uptime (Utils/makeUptimeComputer)
   :version STORM-VERSION
   :worker-thread-pids-atom (atom {})
   :storm-cluster-state (ClusterUtils/mkStormClusterState conf (when (Utils/isZkAuthenticationConfiguredStormServer conf)
                                                     SUPERVISOR-ZK-ACLS)
                                                        (ClusterStateContext. DaemonType/SUPERVISOR))
   :local-state (ConfigUtils/supervisorState conf)
   :supervisor-id (.getSupervisorId isupervisor)
   :assignment-id (.getAssignmentId isupervisor)
   :my-hostname (Utils/hostname conf)
   :curr-assignment (atom nil) ;; used for reporting used ports when heartbeating
   :heartbeat-timer (StormTimer. nil
                      (reify Thread$UncaughtExceptionHandler
                        (^void uncaughtException
                          [this ^Thread t ^Throwable e]
                          (log-error e "Error when processing event")
                          (Utils/exitProcess 20 "Error when processing an event"))))
   :event-timer (StormTimer. nil
                  (reify Thread$UncaughtExceptionHandler
                    (^void uncaughtException
                      [this ^Thread t ^Throwable e]
                      (log-error e "Error when processing event")
                      (Utils/exitProcess 20 "Error when processing an event"))))
   :blob-update-timer (StormTimer. "blob-update-timer"
                        (reify Thread$UncaughtExceptionHandler
                          (^void uncaughtException
                            [this ^Thread t ^Throwable e]
                            (log-error e "Error when processing event")
                            (Utils/exitProcess 20 "Error when processing an event"))))
   :localizer (Utils/createLocalizer conf (ConfigUtils/supervisorLocalDir conf))
   :assignment-versions (atom {})
   :sync-retry (atom 0)
   :download-lock (Object.)
   :stormid->profiler-actions (atom {})
   :resource-isolation-manager (if (conf STORM-RESOURCE-ISOLATION-PLUGIN-ENABLE)
                                 (let [resource-isolation-manager (Utils/newInstance (conf STORM-RESOURCE-ISOLATION-PLUGIN))]
                                   (.prepare resource-isolation-manager conf)
                                   (log-message "Using resource isolation plugin " (conf STORM-RESOURCE-ISOLATION-PLUGIN))
                                   resource-isolation-manager)
                                 nil)
   })

(defn required-topo-files-exist?
  [conf storm-id]
  (let [stormroot (ConfigUtils/supervisorStormDistRoot conf storm-id)
        stormjarpath (ConfigUtils/supervisorStormJarPath stormroot)
        stormcodepath (ConfigUtils/supervisorStormCodePath stormroot)
        stormconfpath (ConfigUtils/supervisorStormConfPath stormroot)]
    (and (every? #(Utils/checkFileExists %) [stormroot stormconfpath stormcodepath])
         (or (ConfigUtils/isLocalMode conf)
             (Utils/checkFileExists stormjarpath)))))

(defn get-worker-assignment-helper-msg
  [assignment supervisor port id]
  (str (pr-str assignment) " for this supervisor " (:supervisor-id supervisor) " on port "
    port " with id " id))

(defn get-valid-new-worker-ids
  [conf supervisor reassign-executors new-worker-ids]
  (into {}
    (remove nil?
      (dofor [[port assignment] reassign-executors]
        (let [id (new-worker-ids port)
              storm-id (:storm-id assignment)
              ^WorkerResources resources (:resources assignment)]
          ;; This condition checks for required files exist before launching the worker
          (if (required-topo-files-exist? conf storm-id)
            (let [pids-path (ConfigUtils/workerPidsRoot conf id)
                  hb-path (ConfigUtils/workerHeartbeatsRoot conf id)]
              (log-message "Launching worker with assignment "
                (get-worker-assignment-helper-msg assignment supervisor port id))
              (FileUtils/forceMkdir (File. pids-path))
              (FileUtils/forceMkdir (File. hb-path))
              (launch-worker supervisor
                (:storm-id assignment)
                port
                id
                resources)
              [id port])
            (do
              (log-message "Missing topology storm code, so can't launch worker with assignment "
                (get-worker-assignment-helper-msg assignment supervisor port id))
              nil)))))))


(defn- select-keys-pred
  [pred amap]
  (into {} (filter (fn [[k v]] (pred k)) amap)))

(defn ->local-assignment
  [^LocalAssignment thrift-local-assignment]
  (mk-local-assignment
    (.get_topology_id thrift-local-assignment)
    (->executor-list (.get_executors thrift-local-assignment))
    (.get_resources thrift-local-assignment)))

;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
(defn ls-local-assignments
  [^LocalState local-state]
  (if-let [thrift-local-assignments (.getLocalAssignmentsMap local-state)]
    (map-val ->local-assignment thrift-local-assignments)))

;TODO: when translating this function, you should replace the filter-val with a proper for loop + if condition HERE
(defn sync-processes [supervisor]
  (let [conf (:conf supervisor)
        ^LocalState local-state (:local-state supervisor)
        storm-cluster-state (:storm-cluster-state supervisor)
        assigned-executors (or (ls-local-assignments local-state) {})
        now (Time/currentTimeSecs)
        allocated (read-allocated-workers supervisor assigned-executors now)
        keepers (filter-val
                 (fn [[state _]] (= state :valid))
                 allocated)
        keep-ports (set (for [[id [_ hb]] keepers] (:port hb)))
        reassign-executors (select-keys-pred (complement keep-ports) assigned-executors)
        new-worker-ids (into
                        {}
                        (for [port (keys reassign-executors)]
                          [port (Utils/uuid)]))]
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
         ". Current supervisor time: " now
         ". State: " state
         ", Heartbeat: " (pr-str heartbeat))
        (shutdown-worker supervisor id)))
    (let [valid-new-worker-ids (get-valid-new-worker-ids conf supervisor reassign-executors new-worker-ids)]
      (.setApprovedWorkers ^LocalState local-state
                        (merge
                          (select-keys (clojurify-structure (.getApprovedWorkers ^LocalState local-state))
                            (keys keepers))
                          valid-new-worker-ids))
      (wait-for-workers-launch conf (keys valid-new-worker-ids)))))

(defn assigned-storm-ids-from-port-assignments [assignment]
  (->> assignment
       vals
       (map :storm-id)
       set))

;TODO: when translating this function, you should replace the filter-val with a proper for loop + if condition HERE
(defn shutdown-disallowed-workers [supervisor]
  (let [conf (:conf supervisor)
        ^LocalState local-state (:local-state supervisor)
        assigned-executors (or (ls-local-assignments local-state) {})
        now (Time/currentTimeSecs)
        allocated (read-allocated-workers supervisor assigned-executors now)
        disallowed (keys (filter-val
                                  (fn [[state _]] (= state :disallowed))
                                  allocated))]
    (log-debug "Allocated workers " allocated)
    (log-debug "Disallowed workers " disallowed)
    (doseq [id disallowed]
      (shutdown-worker supervisor id))
    ))

(defn get-blob-localname
  "Given the blob information either gets the localname field if it exists,
  else routines the default value passed in."
  [blob-info defaultValue]
  (or (get blob-info "localname") defaultValue))

(defn should-uncompress-blob?
  "Given the blob information returns the value of the uncompress field, handling it either being
  a string or a boolean value, or if it's not specified then returns false"
  [blob-info]
  (Boolean. (get blob-info "uncompress")))

(defn remove-blob-references
  "Remove a reference to a blob when its no longer needed."
  [localizer storm-id conf]
  (let [storm-conf (clojurify-structure (ConfigUtils/readSupervisorStormConf conf storm-id))
        blobstore-map (storm-conf TOPOLOGY-BLOBSTORE-MAP)
        user (storm-conf TOPOLOGY-SUBMITTER-USER)
        topo-name (storm-conf TOPOLOGY-NAME)]
    (if blobstore-map
      (doseq [[k, v] blobstore-map]
        (.removeBlobReference localizer
          k
          user
          topo-name
          (should-uncompress-blob? v))))))

(defn blobstore-map-to-localresources
  "Returns a list of LocalResources based on the blobstore-map passed in."
  [blobstore-map]
  (if blobstore-map
    (for [[k, v] blobstore-map] (LocalResource. k (should-uncompress-blob? v)))
    ()))

(defn add-blob-references
  "For each of the downloaded topologies, adds references to the blobs that the topologies are
  using. This is used to reconstruct the cache on restart."
  [localizer storm-id conf]
  (let [storm-conf (clojurify-structure (ConfigUtils/readSupervisorStormConf conf storm-id))
        blobstore-map (storm-conf TOPOLOGY-BLOBSTORE-MAP)
        user (storm-conf TOPOLOGY-SUBMITTER-USER)
        topo-name (storm-conf TOPOLOGY-NAME)
        localresources (blobstore-map-to-localresources blobstore-map)]
    (if blobstore-map
      (.addReferences localizer localresources user topo-name))))

(defn rm-topo-files
  [conf storm-id localizer rm-blob-refs?]
  (let [path (ConfigUtils/supervisorStormDistRoot conf storm-id)]
    (try
      (if rm-blob-refs?
        (remove-blob-references localizer storm-id conf))
      (if (conf SUPERVISOR-RUN-WORKER-AS-USER)
        (rmr-as-user conf storm-id path)
        (Utils/forceDelete (ConfigUtils/supervisorStormDistRoot conf storm-id)))
      (catch Exception e
        (log-message e (str "Exception removing: " storm-id))))))

(defn verify-downloaded-files
  "Check for the files exists to avoid supervisor crashing
   Also makes sure there is no necessity for locking"
  [conf localizer assigned-storm-ids all-downloaded-storm-ids]
  (remove nil?
    (into #{}
      (for [storm-id all-downloaded-storm-ids
            :when (contains? assigned-storm-ids storm-id)]
        (when-not (required-topo-files-exist? conf storm-id)
          (log-debug "Files not present in topology directory")
          (rm-topo-files conf storm-id localizer false)
          storm-id)))))

(defn ->LocalAssignment
  [{storm-id :storm-id executors :executors resources :resources}]
  (let [assignment (LocalAssignment. storm-id (->ExecutorInfo-list executors))]
    (if resources (.set_resources assignment
                                  (doto (WorkerResources. )
                                    (.set_mem_on_heap (first resources))
                                    (.set_mem_off_heap (second resources))
                                    (.set_cpu (last resources)))))
    assignment))

;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
(defn ls-local-assignments!
  [^LocalState local-state assignments]
  (let [local-assignment-map (map-val ->LocalAssignment assignments)]
    (.setLocalAssignmentsMap local-state local-assignment-map)))

(defn mk-synchronize-supervisor [supervisor sync-processes event-manager processes-event-manager]
  (fn callback-supervisor []
    (let [conf (:conf supervisor)
          storm-cluster-state (:storm-cluster-state supervisor)
          ^ISupervisor isupervisor (:isupervisor supervisor)
          ^LocalState local-state (:local-state supervisor)
          sync-callback (fn [] (.add event-manager (reify Runnable
                                                                   (^void run [this]
                                                                     (callback-supervisor)))))
          assignment-versions @(:assignment-versions supervisor)
          {assignments-snapshot :assignments
           storm-id->profiler-actions :profiler-actions
           versions :versions}
          (assignments-snapshot storm-cluster-state sync-callback assignment-versions)

          storm-code-map (read-storm-code-locations assignments-snapshot)
          all-downloaded-storm-ids (set (read-downloaded-storm-ids conf))
          existing-assignment (ls-local-assignments local-state)
          all-assignment (read-assignments assignments-snapshot
                                           (:assignment-id supervisor)
                                           existing-assignment
                                           (:sync-retry supervisor))
          ;TODO: when translating this function, you should replace the filter-val with a proper for loop + if condition HERE
          new-assignment (->> all-assignment
                              (filter-key #(.confirmAssigned isupervisor %)))
          assigned-storm-ids (assigned-storm-ids-from-port-assignments new-assignment)
          localizer (:localizer supervisor)
          checked-downloaded-storm-ids (set (verify-downloaded-files conf localizer assigned-storm-ids all-downloaded-storm-ids))
          downloaded-storm-ids (set/difference all-downloaded-storm-ids checked-downloaded-storm-ids)]

      (log-debug "Synchronizing supervisor")
      (log-debug "Storm code map: " storm-code-map)
      (log-debug "All assignment: " all-assignment)
      (log-debug "New assignment: " new-assignment)
      (log-debug "Assigned Storm Ids " assigned-storm-ids)
      (log-debug "All Downloaded Ids " all-downloaded-storm-ids)
      (log-debug "Checked Downloaded Ids " checked-downloaded-storm-ids)
      (log-debug "Downloaded Ids " downloaded-storm-ids)
      (log-debug "Storm Ids Profiler Actions " storm-id->profiler-actions)
      ;; download code first
      ;; This might take awhile
      ;;   - should this be done separately from usual monitoring?
      ;; should we only download when topology is assigned to this supervisor?
      (doseq [[storm-id master-code-dir] storm-code-map]
        (when (and (not (downloaded-storm-ids storm-id))
                   (assigned-storm-ids storm-id))
          (log-message "Downloading code for storm id " storm-id)
          (try-cause
            (download-storm-code conf storm-id master-code-dir localizer)

            (catch NimbusLeaderNotFoundException e
              (log-warn-error e "Nimbus leader was not available."))
            (catch TTransportException e
              (log-warn-error e "There was a connection problem with nimbus.")))
          (log-message "Finished downloading code for storm id " storm-id)))

      (log-debug "Writing new assignment "
                 (pr-str new-assignment))
      (doseq [p (set/difference (set (keys existing-assignment))
                                (set (keys new-assignment)))]
        (.killedWorker isupervisor (int p)))
      (.assigned isupervisor (keys new-assignment))
      (ls-local-assignments! local-state
            new-assignment)
      (reset! (:assignment-versions supervisor) versions)
      (reset! (:stormid->profiler-actions supervisor) storm-id->profiler-actions)

      (reset! (:curr-assignment supervisor) new-assignment)
      ;; remove any downloaded code that's no longer assigned or active
      ;; important that this happens after setting the local assignment so that
      ;; synchronize-supervisor doesn't try to launch workers for which the
      ;; resources don't exist
      (if (Utils/isOnWindows) (shutdown-disallowed-workers supervisor))
      (doseq [storm-id all-downloaded-storm-ids]
        (when-not (storm-code-map storm-id)
          (log-message "Removing code for storm id "
                       storm-id)
          (rm-topo-files conf storm-id localizer true)))
      (.add processes-event-manager (reify Runnable
                                                     (^void run [this]
                                                       (sync-processes)))))))

(defn mk-supervisor-capacities
  [conf]
  {Config/SUPERVISOR_MEMORY_CAPACITY_MB (double (conf SUPERVISOR-MEMORY-CAPACITY-MB))
   Config/SUPERVISOR_CPU_CAPACITY (double (conf SUPERVISOR-CPU-CAPACITY))})

(defn update-blobs-for-topology!
  "Update each blob listed in the topology configuration if the latest version of the blob
   has not been downloaded."
  [conf storm-id localizer]
  (let [storm-conf (clojurify-structure (ConfigUtils/readSupervisorStormConf conf storm-id))
        blobstore-map (storm-conf TOPOLOGY-BLOBSTORE-MAP)
        user (storm-conf TOPOLOGY-SUBMITTER-USER)
        localresources (blobstore-map-to-localresources blobstore-map)]
    (try
      (.updateBlobs localizer localresources user)
      (catch AuthorizationException authExp
        (log-error authExp))
      (catch KeyNotFoundException knf
        (log-error knf)))))

(defn update-blobs-for-all-topologies-fn
  "Returns a function that downloads all blobs listed in the topology configuration for all topologies assigned
  to this supervisor, and creates version files with a suffix. The returned function is intended to be run periodically
  by a timer, created elsewhere."
  [supervisor]
  (fn []
    (try-cause
      (let [conf (:conf supervisor)
            downloaded-storm-ids (set (read-downloaded-storm-ids conf))
            new-assignment @(:curr-assignment supervisor)
            assigned-storm-ids (assigned-storm-ids-from-port-assignments new-assignment)]
        (doseq [topology-id downloaded-storm-ids]
          (let [storm-root (ConfigUtils/supervisorStormDistRoot conf topology-id)]
            (when (assigned-storm-ids topology-id)
              (log-debug "Checking Blob updates for storm topology id " topology-id " With target_dir: " storm-root)
              (update-blobs-for-topology! conf topology-id (:localizer supervisor))))))
      (catch TTransportException e
        (log-error
          e
          "Network error while updating blobs, will retry again later"))
      (catch NimbusLeaderNotFoundException e
        (log-error
          e
          "Nimbus unavailable to update blobs, will retry again later")))))

(defn jvm-cmd [cmd]
  (let [java-home (.get (System/getenv) "JAVA_HOME")]
    (if (nil? java-home)
      cmd
      (str java-home Utils/FILE_PATH_SEPARATOR "bin" Utils/FILE_PATH_SEPARATOR cmd))))

(defn java-cmd []
  (jvm-cmd "java"))

(defn jmap-dump-cmd [profile-cmd pid target-dir]
  [profile-cmd pid "jmap" target-dir])

(defn jstack-dump-cmd [profile-cmd pid target-dir]
  [profile-cmd pid "jstack" target-dir])

(defn jprofile-start [profile-cmd pid]
  [profile-cmd pid "start"])

(defn jprofile-stop [profile-cmd pid target-dir]
  [profile-cmd pid "stop" target-dir])

(defn jprofile-dump [profile-cmd pid workers-artifacts-directory]
  [profile-cmd pid "dump" workers-artifacts-directory])

(defn jprofile-jvm-restart [profile-cmd pid]
  [profile-cmd pid "kill"])

(defn- delete-topology-profiler-action [storm-cluster-state storm-id profile-action]
  (log-message "Deleting profiler action.." profile-action)
  (.deleteTopologyProfileRequests storm-cluster-state storm-id (thriftify-profile-request profile-action)))

(defnk launch-profiler-action-for-worker
  "Launch profiler action for a worker"
  [conf user target-dir command :environment {} :exit-code-on-profile-action nil :log-prefix nil]
  (if-let [run-worker-as-user (conf SUPERVISOR-RUN-WORKER-AS-USER)]
    (let [container-file (Utils/containerFilePath target-dir)
          script-file (Utils/scriptFilePath target-dir)]
      (log-message "Running as user:" user " command:" (Utils/shellCmd command))
      (if (Utils/checkFileExists container-file) (rmr-as-user conf container-file container-file))
      (if (Utils/checkFileExists script-file) (rmr-as-user conf script-file script-file))
      (worker-launcher
        conf
        user
        ["profiler" target-dir (Utils/writeScript target-dir command environment)]
        :log-prefix log-prefix
        :exit-code-callback exit-code-on-profile-action
        :directory (File. target-dir)))
    (Utils/launchProcess
      command
      environment
      log-prefix
      exit-code-on-profile-action
      (File. target-dir))))

(defn mk-run-profiler-actions-for-all-topologies
  "Returns a function that downloads all profile-actions listed for all topologies assigned
  to this supervisor, executes those actions as user and deletes them from zookeeper."
  [supervisor]
  (fn []
    (try
      (let [conf (:conf supervisor)
            stormid->profiler-actions @(:stormid->profiler-actions supervisor)
            storm-cluster-state (:storm-cluster-state supervisor)
            hostname (:my-hostname supervisor)
            storm-home (System/getProperty "storm.home")
            profile-cmd (str (clojure.java.io/file storm-home
                                                   "bin"
                                                   (conf WORKER-PROFILER-COMMAND)))
            new-assignment @(:curr-assignment supervisor)
            assigned-storm-ids (assigned-storm-ids-from-port-assignments new-assignment)]
        (doseq [[storm-id profiler-actions] stormid->profiler-actions]
          (when (not (empty? profiler-actions))
            (doseq [pro-action profiler-actions]
              (if (= hostname (:host pro-action))
                (let [port (:port pro-action)
                      action ^ProfileAction (:action pro-action)
                      stop? (> (System/currentTimeMillis) (:timestamp pro-action))
                      target-dir (ConfigUtils/workerArtifactsRoot conf storm-id port)
                      storm-conf (clojurify-structure (ConfigUtils/readSupervisorStormConf conf storm-id))
                      user (storm-conf TOPOLOGY-SUBMITTER-USER)
                      environment (if-let [env (storm-conf TOPOLOGY-ENVIRONMENT)] env {})
                      worker-pid (slurp (ConfigUtils/workerArtifactsPidPath conf storm-id port))
                      log-prefix (str "ProfilerAction process " storm-id ":" port " PROFILER_ACTION: " action " ")
                      ;; Until PROFILER_STOP action is invalid, keep launching profiler start in case worker restarted
                      ;; The profiler plugin script validates if JVM is recording before starting another recording.
                      command (cond
                                (= action ProfileAction/JMAP_DUMP) (jmap-dump-cmd profile-cmd worker-pid target-dir)
                                (= action ProfileAction/JSTACK_DUMP) (jstack-dump-cmd profile-cmd worker-pid target-dir)
                                (= action ProfileAction/JPROFILE_DUMP) (jprofile-dump profile-cmd worker-pid target-dir)
                                (= action ProfileAction/JVM_RESTART) (jprofile-jvm-restart profile-cmd worker-pid)
                                (and (not stop?)
                                     (= action ProfileAction/JPROFILE_STOP))
                                  (jprofile-start profile-cmd worker-pid) ;; Ensure the profiler is still running
                                (and stop? (= action ProfileAction/JPROFILE_STOP)) (jprofile-stop profile-cmd worker-pid target-dir))
                      action-on-exit (fn [exit-code]
                                       (log-message log-prefix " profile-action exited for code: " exit-code)
                                       (if stop?
                                         (delete-topology-profiler-action storm-cluster-state storm-id (thriftify-profile-request pro-action))))
                      command (->> command (map str) (filter (complement empty?)))]

                  (try
                    (launch-profiler-action-for-worker conf
                      user
                      target-dir
                      command
                      :environment environment
                      :exit-code-on-profile-action action-on-exit
                      :log-prefix log-prefix)
                    (catch IOException ioe
                      (log-error ioe
                        (str "Error in processing ProfilerAction '" action "' for " storm-id ":" port ", will retry later.")))
                    (catch RuntimeException rte
                      (log-error rte
                        (str "Error in processing ProfilerAction '" action "' for " storm-id ":" port ", will retry later."))))))))))
      (catch Exception e
        (log-error e "Error running profiler actions, will retry again later")))))


(defn is-waiting [^EventManagerImp event-manager]
  (.waiting event-manager))

;; in local state, supervisor stores who its current assignments are
;; another thread launches events to restart any dead processes if necessary
(defserverfn mk-supervisor [conf shared-context ^ISupervisor isupervisor]
  (log-message "Starting Supervisor with conf " conf)
  (.prepare isupervisor conf (ConfigUtils/supervisorIsupervisorDir conf))
  (FileUtils/cleanDirectory (File. (ConfigUtils/supervisorTmpDir conf)))
  (let [supervisor (supervisor-data conf shared-context isupervisor)
        [event-manager processes-event-manager :as managers] [(EventManagerImp. false) (EventManagerImp. false)]
        sync-processes (partial sync-processes supervisor)
        synchronize-supervisor (mk-synchronize-supervisor supervisor sync-processes event-manager processes-event-manager)
        synchronize-blobs-fn (update-blobs-for-all-topologies-fn supervisor)
        downloaded-storm-ids (set (read-downloaded-storm-ids conf))
        run-profiler-actions-fn (mk-run-profiler-actions-for-all-topologies supervisor)
        heartbeat-fn (fn [] (.supervisorHeartbeat
                               (:storm-cluster-state supervisor)
                               (:supervisor-id supervisor)
                               (thriftify-supervisor-info (->SupervisorInfo (Time/currentTimeSecs)
                                                 (:my-hostname supervisor)
                                                 (:assignment-id supervisor)
                                                 (keys @(:curr-assignment supervisor))
                                                  ;; used ports
                                                 (.getMetadata isupervisor)
                                                 (conf SUPERVISOR-SCHEDULER-META)
                                                 (. (:uptime supervisor) upTime)
                                                 (:version supervisor)
                                                 (mk-supervisor-capacities conf)))))]
    (heartbeat-fn)

    ;; should synchronize supervisor so it doesn't launch anything after being down (optimization)
    (.scheduleRecurring (:heartbeat-timer supervisor)
      0
      (conf SUPERVISOR-HEARTBEAT-FREQUENCY-SECS)
      heartbeat-fn)

    (doseq [storm-id downloaded-storm-ids]
      (add-blob-references (:localizer supervisor) storm-id
        conf))
    ;; do this after adding the references so we don't try to clean things being used
    (.startCleaner (:localizer supervisor))

    (when (conf SUPERVISOR-ENABLE)
      ;; This isn't strictly necessary, but it doesn't hurt and ensures that the machine stays up
      ;; to date even if callbacks don't all work exactly right
      (.scheduleRecurring (:event-timer supervisor) 0 10 (fn [] (.add event-manager (reify Runnable
                                                                                      (^void run [this]
                                                                                        (synchronize-supervisor))))))

      (.scheduleRecurring (:event-timer supervisor)
        0
        (conf SUPERVISOR-MONITOR-FREQUENCY-SECS)
        (fn [] (.add processes-event-manager (reify Runnable
                                               (^void run [this]
                                                 (sync-processes))))))

      ;; Blob update thread. Starts with 30 seconds delay, every 30 seconds
      (.scheduleRecurring (:blob-update-timer supervisor)
        30
        30
        (fn [] (.add event-manager (reify Runnable
                                     (^void run [this]
                                       (synchronize-blobs-fn))))))

      (.scheduleRecurring (:event-timer supervisor)
        (* 60 5)
        (* 60 5)
        (fn []
          (let [health-code (HealthCheck/healthCheck conf)
                ids (my-worker-ids conf)]
            (if (not (= health-code 0))
              (do
                (doseq [id ids]
                  (shutdown-worker supervisor id))
                (throw (RuntimeException. "Supervisor failed health check. Exiting.")))))))


      ;; Launch a thread that Runs profiler commands . Starts with 30 seconds delay, every 30 seconds
      (.scheduleRecurring (:event-timer supervisor)
        30
        30
        (fn [] (.add event-manager (reify Runnable
                                     (^void run [this]
                                       (run-profiler-actions-fn))))))
      )
    (log-message "Starting supervisor with id " (:supervisor-id supervisor) " at host " (:my-hostname supervisor))
    (reify
     Shutdownable
     (shutdown [this]
               (log-message "Shutting down supervisor " (:supervisor-id supervisor))
               (reset! (:active supervisor) false)
               (.close (:heartbeat-timer supervisor))
               (.close (:event-timer supervisor))
               (.close (:blob-update-timer supervisor))
               (.close event-manager)
               (.close processes-event-manager)
               (.shutdown (:localizer supervisor))
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
            (.isTimerWaiting (:heartbeat-timer supervisor))
            (.isTimerWaiting (:event-timer supervisor))
            (every? is-waiting managers)))
           ))))



(defn kill-supervisor [supervisor]
  (.shutdown supervisor)
  )

(defn setup-storm-code-dir
  [conf storm-conf dir]
 (if (conf SUPERVISOR-RUN-WORKER-AS-USER)
  (worker-launcher-and-wait conf (storm-conf TOPOLOGY-SUBMITTER-USER) ["code-dir" dir] :log-prefix (str "setup conf for " dir))))

(defn setup-blob-permission
  [conf storm-conf path]
  (if (conf SUPERVISOR-RUN-WORKER-AS-USER)
    (worker-launcher-and-wait conf (storm-conf TOPOLOGY-SUBMITTER-USER) ["blob" path] :log-prefix (str "setup blob permissions for " path))))

(defn download-blobs-for-topology!
  "Download all blobs listed in the topology configuration for a given topology."
  [conf stormconf-path localizer tmproot]
  (let [storm-conf (clojurify-structure (ConfigUtils/readSupervisorStormConfGivenPath conf stormconf-path))
        blobstore-map (storm-conf TOPOLOGY-BLOBSTORE-MAP)
        user (storm-conf TOPOLOGY-SUBMITTER-USER)
        topo-name (storm-conf TOPOLOGY-NAME)
        user-dir (.getLocalUserFileCacheDir localizer user)
        localresources (blobstore-map-to-localresources blobstore-map)]
    (when localresources
      (when-not (.exists user-dir)
        (FileUtils/forceMkdir user-dir))
      (try
        (let [localized-resources (.getBlobs localizer localresources user topo-name user-dir)]
          (setup-blob-permission conf storm-conf (.toString user-dir))
          (doseq [local-rsrc localized-resources]
            (let [rsrc-file-path (File. (.getFilePath local-rsrc))
                  key-name (.getName rsrc-file-path)
                  blob-symlink-target-name (.getName (File. (.getCurrentSymlinkPath local-rsrc)))
                  symlink-name (get-blob-localname (get blobstore-map key-name) key-name)]
              (Utils/createSymlink tmproot (.getParent rsrc-file-path) symlink-name
                blob-symlink-target-name))))
        (catch AuthorizationException authExp
          (log-error authExp))
        (catch KeyNotFoundException knf
          (log-error knf))))))

(defn get-blob-file-names
  [blobstore-map]
  (if blobstore-map
    (for [[k, data] blobstore-map]
      (get-blob-localname data k))))

(defn download-blobs-for-topology-succeed?
  "Assert if all blobs are downloaded for the given topology"
  [stormconf-path target-dir]
  (let [storm-conf (clojurify-structure (Utils/fromCompressedJsonConf (FileUtils/readFileToByteArray (File. stormconf-path))))
        blobstore-map (storm-conf TOPOLOGY-BLOBSTORE-MAP)
        file-names (get-blob-file-names blobstore-map)]
    (if-not (empty? file-names)
      (every? #(Utils/checkFileExists target-dir %) file-names)
      true)))

;; distributed implementation
(defmethod download-storm-code
  :distributed [conf storm-id master-code-dir localizer]
  ;; Downloading to permanent location is atomic

  (let [tmproot (str (ConfigUtils/supervisorTmpDir conf) Utils/FILE_PATH_SEPARATOR (Utils/uuid))
        stormroot (ConfigUtils/supervisorStormDistRoot conf storm-id)
        blobstore (Utils/getClientBlobStoreForSupervisor conf)]
    (FileUtils/forceMkdir (File. tmproot))
    (if-not (Utils/isOnWindows)
      (Utils/restrictPermissions tmproot)
      (if (conf SUPERVISOR-RUN-WORKER-AS-USER)
        (throw (RuntimeException. (str "ERROR: Windows doesn't implement setting the correct permissions")))))
    (Utils/downloadResourcesAsSupervisor (ConfigUtils/masterStormJarKey storm-id)
      (ConfigUtils/supervisorStormJarPath tmproot) blobstore)
    (Utils/downloadResourcesAsSupervisor (ConfigUtils/masterStormCodeKey storm-id)
      (ConfigUtils/supervisorStormCodePath tmproot) blobstore)
    (Utils/downloadResourcesAsSupervisor (ConfigUtils/masterStormConfKey storm-id)
      (ConfigUtils/supervisorStormConfPath tmproot) blobstore)
    (.shutdown blobstore)
    (Utils/extractDirFromJar (ConfigUtils/supervisorStormJarPath tmproot) ConfigUtils/RESOURCES_SUBDIR tmproot)
    (download-blobs-for-topology! conf (ConfigUtils/supervisorStormConfPath tmproot) localizer
      tmproot)
    (if (download-blobs-for-topology-succeed? (ConfigUtils/supervisorStormConfPath tmproot) tmproot)
      (do
        (log-message "Successfully downloaded blob resources for storm-id " storm-id)
        (FileUtils/forceMkdir (File. stormroot))
        (Files/move (.toPath (File. tmproot)) (.toPath (File. stormroot))
          (doto (make-array StandardCopyOption 1) (aset 0 StandardCopyOption/ATOMIC_MOVE)))
        (setup-storm-code-dir conf (clojurify-structure (ConfigUtils/readSupervisorStormConf conf storm-id)) stormroot))
      (do
        (log-message "Failed to download blob resources for storm-id " storm-id)
        (Utils/forceDelete tmproot)))))

(defn write-log-metadata-to-yaml-file! [storm-id port data conf]
  (let [file (ConfigUtils/getLogMetaDataFile conf storm-id port)]
    ;;run worker as user needs the directory to have special permissions
    ;; or it is insecure
    (when (not (.exists (.getParentFile file)))
      (if (conf SUPERVISOR-RUN-WORKER-AS-USER)
        (do (FileUtils/forceMkdir (.getParentFile file))
            (setup-storm-code-dir
              conf
              (clojurify-structure (ConfigUtils/readSupervisorStormConf conf storm-id))
              (.getCanonicalPath (.getParentFile file))))
        (.mkdirs (.getParentFile file))))
    (let [writer (java.io.FileWriter. file)
          yaml (Yaml.)]
      (try
        (.dump yaml data writer)
        (finally
          (.close writer))))))

(defn write-log-metadata! [storm-conf user worker-id storm-id port conf]
  (let [data {TOPOLOGY-SUBMITTER-USER user
              "worker-id" worker-id
              LOGS-GROUPS (sort (distinct (remove nil?
                                           (concat
                                             (storm-conf LOGS-GROUPS)
                                             (storm-conf TOPOLOGY-GROUPS)))))
              LOGS-USERS (sort (distinct (remove nil?
                                           (concat
                                             (storm-conf LOGS-USERS)
                                             (storm-conf TOPOLOGY-USERS)))))}]
    (write-log-metadata-to-yaml-file! storm-id port data conf)))

(defn jlp [stormroot conf]
  (let [resource-root (str stormroot File/separator ConfigUtils/RESOURCES_SUBDIR)
        os (clojure.string/replace (System/getProperty "os.name") #"\s+" "_")
        arch (System/getProperty "os.arch")
        arch-resource-root (str resource-root File/separator os "-" arch)]
    (str arch-resource-root File/pathSeparator resource-root File/pathSeparator (conf JAVA-LIBRARY-PATH))))

(defn substitute-childopts
  "Generates runtime childopts by replacing keys with topology-id, worker-id, port, mem-onheap"
  [value worker-id topology-id port mem-onheap]
  (let [replacement-map {"%ID%"          (str port)
                         "%WORKER-ID%"   (str worker-id)
                         "%TOPOLOGY-ID%"    (str topology-id)
                         "%WORKER-PORT%" (str port)
                         "%HEAP-MEM%" (str mem-onheap)}
        sub-fn #(reduce (fn [string entry]
                          (apply clojure.string/replace string entry))
                        %
                        replacement-map)]
    (cond
      (nil? value) nil
      (sequential? value) (vec (map sub-fn value))
      :else (-> value sub-fn (clojure.string/split #"\s+")))))


(defn create-blobstore-links
  "Create symlinks in worker launch directory for all blobs"
  [conf storm-id worker-id]
  (let [stormroot (ConfigUtils/supervisorStormDistRoot conf storm-id)
        storm-conf (clojurify-structure (ConfigUtils/readSupervisorStormConf conf storm-id))
        workerroot (ConfigUtils/workerRoot conf worker-id)
        blobstore-map (storm-conf TOPOLOGY-BLOBSTORE-MAP)
        blob-file-names (get-blob-file-names blobstore-map)
        resource-file-names (cons ConfigUtils/RESOURCES_SUBDIR blob-file-names)]
    (log-message "Creating symlinks for worker-id: " worker-id " storm-id: "
      storm-id " for files(" (count resource-file-names) "): " (pr-str resource-file-names))
    (Utils/createSymlink workerroot stormroot ConfigUtils/RESOURCES_SUBDIR)
    (doseq [file-name blob-file-names]
      (Utils/createSymlink workerroot stormroot file-name file-name))))

(defn create-artifacts-link
  "Create a symlink from workder directory to its port artifacts directory"
  [conf storm-id port worker-id]
  (let [worker-dir (ConfigUtils/workerRoot conf worker-id)
        topo-dir (ConfigUtils/workerArtifactsRoot conf storm-id)]
    (log-message "Creating symlinks for worker-id: " worker-id " storm-id: "
                 storm-id " to its port artifacts directory")
    (if (.exists (File. worker-dir))
      (Utils/createSymlink worker-dir topo-dir "artifacts" (str port)))))

(defmethod launch-worker
    :distributed [supervisor storm-id port worker-id resources]
    (let [conf (:conf supervisor)
          run-worker-as-user (conf SUPERVISOR-RUN-WORKER-AS-USER)
          storm-home (System/getProperty "storm.home")
          storm-options (System/getProperty "storm.options")
          storm-conf-file (System/getProperty "storm.conf.file")
          storm-log-dir (ConfigUtils/getLogDir)
          storm-log-conf-dir (conf STORM-LOG4J2-CONF-DIR)
          storm-log4j2-conf-dir (if storm-log-conf-dir
                                  (if (.isAbsolute (File. storm-log-conf-dir))
                                    storm-log-conf-dir
                                    (str storm-home Utils/FILE_PATH_SEPARATOR storm-log-conf-dir))
                                  (str storm-home Utils/FILE_PATH_SEPARATOR "log4j2"))
          stormroot (ConfigUtils/supervisorStormDistRoot conf storm-id)
          jlp (jlp stormroot conf)
          stormjar (ConfigUtils/supervisorStormJarPath stormroot)
          storm-conf (clojurify-structure (ConfigUtils/readSupervisorStormConf conf storm-id))
          topo-classpath (if-let [cp (storm-conf TOPOLOGY-CLASSPATH)]
                           [cp]
                           [])
          classpath (-> (Utils/workerClasspath)
                        (Utils/addToClasspath [stormjar])
                        (Utils/addToClasspath topo-classpath))
          top-gc-opts (storm-conf TOPOLOGY-WORKER-GC-CHILDOPTS)

          mem-onheap (if (and (.get_mem_on_heap resources) (> (.get_mem_on_heap resources) 0)) ;; not nil and not zero
                       (int (Math/ceil (.get_mem_on_heap resources))) ;; round up
                       (storm-conf WORKER-HEAP-MEMORY-MB)) ;; otherwise use default value

          mem-offheap (int (Math/ceil (.get_mem_off_heap resources)))

          cpu (int (Math/ceil (.get_cpu resources)))

          gc-opts (substitute-childopts (if top-gc-opts top-gc-opts (conf WORKER-GC-CHILDOPTS)) worker-id storm-id port mem-onheap)
          topo-worker-logwriter-childopts (storm-conf TOPOLOGY-WORKER-LOGWRITER-CHILDOPTS)
          user (storm-conf TOPOLOGY-SUBMITTER-USER)
          logfilename "worker.log"
          workers-artifacts (ConfigUtils/workerArtifactsRoot conf)
          logging-sensitivity (storm-conf TOPOLOGY-LOGGING-SENSITIVITY "S3")
          worker-childopts (when-let [s (conf WORKER-CHILDOPTS)]
                             (substitute-childopts s worker-id storm-id port mem-onheap))
          topo-worker-childopts (when-let [s (storm-conf TOPOLOGY-WORKER-CHILDOPTS)]
                                  (substitute-childopts s worker-id storm-id port mem-onheap))
          worker--profiler-childopts (if (conf WORKER-PROFILER-ENABLED)
                                       (substitute-childopts (conf WORKER-PROFILER-CHILDOPTS) worker-id storm-id port mem-onheap)
                                       "")
          topology-worker-environment (if-let [env (storm-conf TOPOLOGY-ENVIRONMENT)]
                                        (merge env {"LD_LIBRARY_PATH" jlp})
                                        {"LD_LIBRARY_PATH" jlp})

          log4j-configuration-file (str (if (.startsWith (System/getProperty "os.name") "Windows")
                                          (if (.startsWith storm-log4j2-conf-dir "file:")
                                            storm-log4j2-conf-dir
                                            (str "file:///" storm-log4j2-conf-dir))
                                          storm-log4j2-conf-dir)
                                     Utils/FILE_PATH_SEPARATOR "worker.xml")

          command (concat
                    [(java-cmd) "-cp" classpath
                     topo-worker-logwriter-childopts
                     (str "-Dlogfile.name=" logfilename)
                     (str "-Dstorm.home=" storm-home)
                     (str "-Dworkers.artifacts=" workers-artifacts)
                     (str "-Dstorm.id=" storm-id)
                     (str "-Dworker.id=" worker-id)
                     (str "-Dworker.port=" port)
                     (str "-Dstorm.log.dir=" storm-log-dir)
                     (str "-Dlog4j.configurationFile=" log4j-configuration-file)
                     (str "-DLog4jContextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector")
                     "org.apache.storm.LogWriter"]
                    [(java-cmd) "-server"]
                    worker-childopts
                    topo-worker-childopts
                    gc-opts
                    worker--profiler-childopts
                    [(str "-Djava.library.path=" jlp)
                     (str "-Dlogfile.name=" logfilename)
                     (str "-Dstorm.home=" storm-home)
                     (str "-Dworkers.artifacts=" workers-artifacts)
                     (str "-Dstorm.conf.file=" storm-conf-file)
                     (str "-Dstorm.options=" storm-options)
                     (str "-Dstorm.log.dir=" storm-log-dir)
                     (str "-Dlogging.sensitivity=" logging-sensitivity)
                     (str "-Dlog4j.configurationFile=" log4j-configuration-file)
                     (str "-DLog4jContextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector")
                     (str "-Dstorm.id=" storm-id)
                     (str "-Dworker.id=" worker-id)
                     (str "-Dworker.port=" port)
                     "-cp" classpath
                     "org.apache.storm.daemon.worker"
                     storm-id
                     (:assignment-id supervisor)
                     port
                     worker-id])
          command (->> command
                       (map str)
                       (filter (complement empty?)))
          command (if (conf STORM-RESOURCE-ISOLATION-PLUGIN-ENABLE)
                    (do
                      (.reserveResourcesForWorker (:resource-isolation-manager supervisor) worker-id
                        {"cpu" cpu "memory" (+ mem-onheap mem-offheap  (int (Math/ceil (conf STORM-CGROUP-MEMORY-LIMIT-TOLERANCE-MARGIN-MB))))})
                      (.getLaunchCommand (:resource-isolation-manager supervisor) worker-id
                        (java.util.ArrayList. (java.util.Arrays/asList (to-array command)))))
                    command)]
      (log-message "Launching worker with command: " (Utils/shellCmd command))
      (write-log-metadata! storm-conf user worker-id storm-id port conf)
      (ConfigUtils/setWorkerUserWSE conf worker-id user)
      (create-artifacts-link conf storm-id port worker-id)
      (let [log-prefix (str "Worker Process " worker-id)
            callback (reify Utils$ExitCodeCallable
                       (call [this exit-code]
                         (log-message log-prefix " exited with code: " exit-code)
                         (add-dead-worker worker-id)))
            worker-dir (ConfigUtils/workerRoot conf worker-id)]
        (remove-dead-worker worker-id)
        (create-blobstore-links conf storm-id worker-id)
        (if run-worker-as-user
          (worker-launcher conf user ["worker" worker-dir (Utils/writeScript worker-dir command topology-worker-environment)] :log-prefix log-prefix :exit-code-callback callback :directory (File. worker-dir))
          (Utils/launchProcess command
                               topology-worker-environment
                               log-prefix
                               callback
                               (File. worker-dir))))))

;; local implementation

(defn resources-jar []
  (->> (.split (Utils/currentClasspath) File/pathSeparator)
       (filter #(.endsWith  % ".jar"))
       (filter #(Utils/zipDoesContainDir % ConfigUtils/RESOURCES_SUBDIR))
       first ))

(defmethod download-storm-code
  :local [conf storm-id master-code-dir localizer]
  (let [tmproot (str (ConfigUtils/supervisorTmpDir conf) Utils/FILE_PATH_SEPARATOR (Utils/uuid))
        stormroot (ConfigUtils/supervisorStormDistRoot conf storm-id)
        blob-store (Utils/getNimbusBlobStore conf master-code-dir nil)]
    (try
      (FileUtils/forceMkdir (File. tmproot))
      (.readBlobTo blob-store (ConfigUtils/masterStormCodeKey storm-id) (FileOutputStream. (ConfigUtils/supervisorStormCodePath tmproot)) nil)
      (.readBlobTo blob-store (ConfigUtils/masterStormConfKey storm-id) (FileOutputStream. (ConfigUtils/supervisorStormConfPath tmproot)) nil)
      (finally
        (.shutdown blob-store)))
    (FileUtils/moveDirectory (File. tmproot) (File. stormroot))

    (setup-storm-code-dir conf (clojurify-structure (ConfigUtils/readSupervisorStormConf conf storm-id)) stormroot)
    (let [classloader (.getContextClassLoader (Thread/currentThread))
          resources-jar (resources-jar)
          url (.getResource classloader ConfigUtils/RESOURCES_SUBDIR)
          target-dir (str stormroot Utils/FILE_PATH_SEPARATOR ConfigUtils/RESOURCES_SUBDIR)]
      (cond
        resources-jar
        (do
          (log-message "Extracting resources from jar at " resources-jar " to " target-dir)
          (Utils/extractDirFromJar resources-jar ConfigUtils/RESOURCES_SUBDIR stormroot))
        url
        (do
          (log-message "Copying resources at " (str url) " to " target-dir)
          (FileUtils/copyDirectory (File. (.getFile url)) (File. target-dir)))))))

(defmethod launch-worker
    :local [supervisor storm-id port worker-id resources]
    (let [conf (:conf supervisor)
          pid (Utils/uuid)
          worker (worker/mk-worker conf
                                   (:shared-context supervisor)
                                   storm-id
                                   (:assignment-id supervisor)
                                   port
                                   worker-id)]
      (ConfigUtils/setWorkerUserWSE conf worker-id "")
      (ProcessSimulator/registerProcess pid worker)
      (swap! (:worker-thread-pids-atom supervisor) assoc worker-id pid)
      ))

(defn -launch
  [supervisor]
  (log-message "Starting supervisor for storm version '" STORM-VERSION "'")
  (let [conf (clojurify-structure (ConfigUtils/readStormConfig))]
    (validate-distributed-mode! conf)
    (let [supervisor (mk-supervisor conf nil supervisor)]
      (Utils/addShutdownHookWithForceKillIn1Sec #(.shutdown supervisor)))
    (defgauge supervisor:num-slots-used-gauge #(count (my-worker-ids conf)))
    (start-metrics-reporters conf)))

(defn standalone-supervisor []
  (let [conf-atom (atom nil)
        id-atom (atom nil)]
    (reify ISupervisor
      (prepare [this conf local-dir]
        (reset! conf-atom conf)
        (let [state (LocalState. local-dir)
              curr-id (if-let [id (.getSupervisorId state)]
                        id
                        (generate-supervisor-id))]
          (.setSupervisorId state curr-id)
          (reset! id-atom curr-id))
        )
      (confirmAssigned [this port]
        true)
      (getMetadata [this]
        (doall (map int (get @conf-atom SUPERVISOR-SLOTS-PORTS))))
      (getSupervisorId [this]
        @id-atom)
      (getAssignmentId [this]
        @id-atom)
      (killedWorker [this port]
        )
      (assigned [this ports]
        ))))

(defn -main []
  (Utils/setupDefaultUncaughtExceptionHandler)
  (-launch (standalone-supervisor)))
