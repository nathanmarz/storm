(ns backtype.storm.daemon.nimbus
  (:import [org.apache.thrift7.server THsHaServer THsHaServer$Args])
  (:import [org.apache.thrift7.protocol TBinaryProtocol TBinaryProtocol$Factory])
  (:import [org.apache.thrift7 TException])
  (:import [org.apache.thrift7.transport TNonblockingServerTransport TNonblockingServerSocket])
  (:import [java.nio ByteBuffer])
  (:import [java.nio.channels Channels WritableByteChannel])
  (:use [backtype.storm bootstrap])
  (:use [backtype.storm.daemon common])
  (:gen-class))

(bootstrap)

(defmulti setup-jar cluster-mode)


;; Master:
;; job submit:
;; 1. read which nodes are available
;; 2. set up the worker/{storm}/{task} stuff (static)
;; 3. set assignments
;; 4. start storm - necessary in case master goes down, when goes back up can remember to take down the storm (2 states: on or off)

;; Monitoring (or by checking when nodes go down or heartbeats aren't received):
;; 1. read assignment
;; 2. see which tasks/nodes are up
;; 3. make new assignment to fix any problems
;; 4. if a storm exists but is not taken down fully, ensure that storm takedown is launched (step by step remove tasks and finally remove assignments)

(defn- assigned-slots
  "Returns a map from node-id to a set of ports"
  [storm-cluster-state]
  (let [assignments (.assignments storm-cluster-state nil)
        ]
    (defaulted
      (apply merge-with set/union
             (for [a assignments
                   [_ [node port]] (-> (.assignment-info storm-cluster-state a nil) :task->node+port)]
               {node #{port}}
               ))
      {})
    ))

(defn- all-supervisor-info
  ([storm-cluster-state] (all-supervisor-info storm-cluster-state nil))
  ([storm-cluster-state callback]
     (let [supervisor-ids (.supervisors storm-cluster-state callback)]
       (into {}
             (mapcat
              (fn [id]
                (if-let [info (.supervisor-info storm-cluster-state id)]
                  [[id info]]
                  ))
              supervisor-ids))
       )))

(defn- available-slots
  [conf storm-cluster-state callback]
  (let [supervisor-ids (.supervisors storm-cluster-state callback)
        supervisor-infos (all-supervisor-info storm-cluster-state callback)
        ;; TODO: this is broken. need to maintain a map since last time
        ;; supervisor hearbeats like is done for tasks
        ;; maybe it's ok to trust ephemeral nodes here?
        ;;[[id info]]
        ;; (when (< (time-delta (:time-secs info))
        ;;          (conf NIMBUS-SUPERVISOR-TIMEOUT-SECS))
        ;;   [[id info]]
        ;;   )        
        all-slots (map-val (comp set :worker-ports) supervisor-infos)
        existing-slots (assigned-slots storm-cluster-state)
        ]
    [(map-val :hostname supervisor-infos)
     (mapcat
       (fn [[id slots]]
         (for [s (set/difference slots (existing-slots id))]
           [id s]))
       all-slots)
      ]))

(defn state-spout-parallelism [state-spout-spec]
  (-> state-spout-spec .get_common thrift/parallelism-hint))

(defn- spout-parallelism [spout-spec]
  (if (.is_distributed spout-spec)
    (-> spout-spec .get_common thrift/parallelism-hint)
    1 ))

(defn bolt-parallelism [bolt-spec]
  (let [hint (-> bolt-spec .get_common thrift/parallelism-hint)
        fully-global? (every?
                       thrift/global-grouping?
                       (vals (.get_inputs bolt-spec)))]
    (if fully-global?
      1
      hint
      )))

(defn- optimize-topology [topology]
  ;; TODO: create new topology by collapsing bolts into CompoundSpout
  ;; and CompoundBolt
  ;; need to somehow maintain stream/component ids inside tuples
  topology)

(defn mk-task-maker [max-parallelism parallelism-func id-counter]
  (fn [[component-id spec]]
    (let [parallelism (parallelism-func spec)
          parallelism (if max-parallelism (min parallelism max-parallelism) parallelism)
          num-tasks (max 1 parallelism)]
      (for-times num-tasks
                 [(id-counter) component-id])
      )))

(defn- setup-storm-code [conf storm-id tmp-jar-location storm-conf topology]
  (let [stormroot (master-stormdist-root conf storm-id)]
   (FileUtils/forceMkdir (File. stormroot))
   (FileUtils/cleanDirectory (File. stormroot))
   (setup-jar conf tmp-jar-location stormroot)
   (FileUtils/writeByteArrayToFile (File. (master-stormcode-path stormroot)) (Utils/serialize topology))
   (FileUtils/writeByteArrayToFile (File. (master-stormconf-path stormroot)) (Utils/serialize storm-conf))
   ))


(defn- read-storm-conf [conf storm-id]
  (let [stormroot (master-stormdist-root conf storm-id)]
    (merge conf
           (Utils/deserialize
            (FileUtils/readFileToByteArray
             (File. (master-stormconf-path stormroot))
             )))))

(defn- read-storm-topology [conf storm-id]
  (let [stormroot (master-stormdist-root conf storm-id)]
    (Utils/deserialize
      (FileUtils/readFileToByteArray
        (File. (master-stormcode-path stormroot))
        ))))


(defn task-dead? [conf storm-cluster-state storm-id task-id]
  (let [info (.task-heartbeat storm-cluster-state storm-id task-id)]
    (or (not info)
        (> (time-delta (:time-secs info))
           (conf NIMBUS-TASK-TIMEOUT-SECS)))
    ))

;; public so it can be mocked in tests
(defn mk-task-component-assignments [conf storm-id]
  (let [storm-conf (read-storm-conf conf storm-id)
        max-parallelism (storm-conf TOPOLOGY-MAX-TASK-PARALLELISM)
        topology (read-storm-topology conf storm-id)
        slots-to-use (storm-conf TOPOLOGY-WORKERS)
        counter (mk-counter)
        tasks (concat
               (mapcat (mk-task-maker max-parallelism bolt-parallelism counter)
                       (.get_bolts topology))
               (mapcat (mk-task-maker max-parallelism spout-parallelism counter)
                       (.get_spouts topology))
               (mapcat (mk-task-maker max-parallelism state-spout-parallelism counter)
                       (.get_state_spouts topology))
               (repeatedly (storm-conf TOPOLOGY-ACKERS)
                           (fn [] [(counter) ACKER-COMPONENT-ID]))
               )]
    (into {}
      tasks)
    ))

(defn- setup-storm-static [conf storm-id storm-cluster-state]
  (doseq [[task-id component-id] (mk-task-component-assignments conf storm-id)]
    (.set-task! storm-cluster-state storm-id task-id (TaskInfo. component-id))
    ))


;; Does not assume that clocks are synchronized. Task heartbeat is only used so that
;; nimbus knows when it's received a new heartbeat. All timing is done by nimbus and
;; tracked through task-heartbeat-cache
(defn- alive-tasks [conf storm-id storm-cluster-state task-ids task-start-times task-heartbeats-cache]
  (doall
    (filter
      (fn [task-id]
        (let [heartbeat (.task-heartbeat storm-cluster-state storm-id task-id)
              reported-time (:time-secs heartbeat)
              {last-nimbus-time :nimbus-time
               last-reported-time :task-reported-time} (get-in @task-heartbeats-cache
                                                               [storm-id task-id])
              task-start-time (get task-start-times task-id)
              nimbus-time (if (or (not last-nimbus-time)
                                  (not= last-reported-time reported-time))
                            (current-time-secs)
                            last-nimbus-time
                            )
              ]          
          (swap! task-heartbeats-cache
                 assoc-in [storm-id task-id]
                 {:nimbus-time nimbus-time
                  :task-reported-time reported-time})
          (if (and task-start-time
                   (or
                    (< (time-delta task-start-time)
                       (conf NIMBUS-TASK-LAUNCH-SECS))
                    (not nimbus-time)
                    (< (time-delta nimbus-time)
                       (conf NIMBUS-TASK-TIMEOUT-SECS))
                    ))
            true
            (do
              (log-message "Task " storm-id ":" task-id " timed out")
              false)
            )))
      task-ids
      )))

(defn- keeper-slots [existing-slots num-task-ids num-workers]
  (if (= 0 num-workers)
    {}
    (let [distribution (atom (integer-divided num-task-ids num-workers))
          keepers (atom {})]
      (doseq [[node+port task-list] existing-slots :let [task-count (count task-list)]]
        (when (pos? (get @distribution task-count 0))
          (swap! keepers assoc node+port task-list)
          (swap! distribution update-in [task-count] dec)
          ))
      @keepers
      )))


(defn sort-slots [all-slots]
  (let [split-up (vals (group-by first all-slots))]
    (apply interleave-all split-up)
    ))

;; NEW NOTES
;; only assign to supervisors who are there and haven't timed out
;; need to reassign workers with tasks that have timed out (will this make it brittle?)
;; need to read in the topology and storm-conf from disk
;; if no slots available and no slots used by this storm, just skip and do nothing
;; otherwise, package rest of tasks into available slots (up to how much it needs)

;; in the future could allocate tasks intelligently (so that "close" tasks reside on same machine)


;; TODO: slots that have dead task should be reused as long as supervisor is active

;; public so it can be mocked out
(defn compute-new-task->node+port [conf storm-id existing-assignment storm-cluster-state available-slots callback task-heartbeats-cache]
  (let [existing-assigned (reverse-map (:task->node+port existing-assignment))
        storm-conf (read-storm-conf conf storm-id)
        all-task-ids (set (.task-ids storm-cluster-state storm-id))
        alive-ids (set (alive-tasks conf storm-id storm-cluster-state
                                    all-task-ids (:task->start-time-secs existing-assignment) task-heartbeats-cache))
        alive-assigned (filter-val (partial every? alive-ids) existing-assigned)
        alive-node-ids (map first (keys alive-assigned))
        total-slots-to-use (min (storm-conf TOPOLOGY-WORKERS)
                                (+ (count available-slots) (count alive-assigned)))
        keep-assigned (keeper-slots alive-assigned (count all-task-ids) total-slots-to-use)
        freed-slots (keys (apply dissoc alive-assigned (keys keep-assigned)))
        reassign-slots (take (- total-slots-to-use (count keep-assigned))
                             (sort-slots (concat available-slots freed-slots)))
        reassign-ids (sort (set/difference all-task-ids (set (apply concat (vals keep-assigned)))))
        reassignment (into {}
                           (map vector
                                reassign-ids
                                ;; for some reason it goes into infinite loop without limiting the repeat-seq
                                (repeat-seq (count reassign-ids) reassign-slots)))
        stay-assignment (into {} (mapcat (fn [[node+port task-ids]] (for [id task-ids] [id node+port])) keep-assigned))]
    (when-not (empty? reassignment)
      (log-message "Reassigning " storm-id " to " total-slots-to-use " slots")
      (log-message "Reassign ids: " (vec reassign-ids))
      (log-message "Available slots: " (pr-str available-slots))
      )
    (merge stay-assignment reassignment)
    ))


(defn changed-ids [task->node+port new-task->node+port]
  (let [slot-assigned (reverse-map task->node+port)
        new-slot-assigned (reverse-map new-task->node+port)
        brand-new-slots (map-diff slot-assigned new-slot-assigned)]
    (apply concat (vals brand-new-slots))
    ))

;; get existing assignment (just the task->node+port map) -> default to {}
;; filter out ones which have a task timeout
;; figure out available slots on cluster. add to that the used valid slots to get total slots. figure out how many tasks should be in each slot (e.g., 4, 4, 4, 5)
;; only keep existing slots that satisfy one of those slots. for rest, reassign them across remaining slots
;; edge case for slots with no task timeout but with supervisor timeout... just treat these as valid slots that can be reassigned to. worst comes to worse the task will timeout and won't assign here next time around
(defn- mk-assignments [conf storm-id storm-cluster-state callback task-heartbeats-cache]
  (log-debug "Determining assignment for " storm-id)
  (let [existing-assignment (.assignment-info storm-cluster-state storm-id nil)
        [node->host available-slots] (available-slots conf storm-cluster-state callback)
        task->node+port (compute-new-task->node+port conf storm-id existing-assignment
                          storm-cluster-state available-slots callback task-heartbeats-cache)
        all-node->host (merge (:node->host existing-assignment) node->host)
        reassign-ids (changed-ids (:task->node+port existing-assignment) task->node+port)
        now-secs (current-time-secs)
        start-times (merge (:task->start-time-secs existing-assignment)
                           (into {}
                             (for [id reassign-ids]
                               [id now-secs]
                               )))

        assignment (Assignment.
                    (master-stormdist-root conf storm-id)
                    (select-keys all-node->host (map first (vals task->node+port)))
                    task->node+port
                    start-times
                    )
        ]
    ;; tasks figure out what tasks to talk to by looking at topology at runtime
    ;; only log/set when there's been a change to the assignment
    (if (= existing-assignment assignment)
      (log-debug "Assignment for " storm-id " hasn't changed")
      (do
        (log-message "Setting new assignment for storm id " storm-id ": " (pr-str assignment))
        (.set-assignment! storm-cluster-state storm-id assignment)
        ))
    ))

(defn- start-storm [storm-name storm-cluster-state storm-id]
  (log-message "Activating " storm-name ": " storm-id)
  (.activate-storm! storm-cluster-state storm-id (StormBase. storm-name (current-time-secs) {:type :active}))
  )

;; Master:
;; job submit:
;; 1. read which nodes are available
;; 2. set up the worker/{storm}/{task} stuff (static)
;; 3. set assignments
;; 4. start storm - necessary in case master goes down, when goes back up can remember to take down the storm (2 states: on or off)

(defn storm-active? [storm-cluster-state storm-name]
  (not-nil? (get-storm-id storm-cluster-state storm-name)))

(defn topologies-to-kill [storm-cluster-state]
  (let [assigned-ids (set (.assignments storm-cluster-state nil))]
    (reduce (fn [ret id]
              (let [status (:status (.storm-base storm-cluster-state id nil))]
                (if (or (nil? status) (= :killed (:type status)))
                  (assoc ret id (:kill-time-secs status))
                  ret)
                ))
            {}
            assigned-ids)
        ))

(defn cleanup-storm-ids [conf storm-cluster-state]
  (let [heartbeat-ids (set (.heartbeat-storms storm-cluster-state))
        error-ids (set (.task-error-storms storm-cluster-state))
        assigned-ids (set (.assignments storm-cluster-state nil))
        storm-ids (set/difference (set/union heartbeat-ids error-ids) assigned-ids)]
    (filter
      (fn [storm-id]
        (every?
          (partial task-dead? conf storm-cluster-state storm-id)
          (.heartbeat-tasks storm-cluster-state storm-id)
          ))
      storm-ids
      )))

(defn validate-topology! [topology]
  (let [bolt-ids (keys (.get_bolts topology))
        spout-ids (keys (.get_spouts topology))
        state-spout-ids (keys (.get_state_spouts topology))
        common (any-intersection bolt-ids spout-ids state-spout-ids)]
    (when-not (empty? common)
      (throw
       (InvalidTopologyException.
        (str "Cannot use same component id for both spout and bolt: " (vec common))
        )))
    (when-not (every? #(> % 0) (concat bolt-ids spout-ids state-spout-ids))
      (throw
       (InvalidTopologyException.
        "All component ids must be positive")))
    ;; TODO: validate that every declared stream is positive
    ))

(defn file-cache-map [conf]
  (TimeCacheMap.
   (int (conf NIMBUS-FILE-COPY-EXPIRATION-SECS))
   (reify TimeCacheMap$ExpiredCallback
          (expire [this id stream]
                  (.close stream)
                  ))
   ))

(defn extract-status-str [base]
  (let [t (-> base :status :type)]
    (.toUpperCase (name t))
    ))

(defserverfn service-handler [conf]
  (let [submitted-count (atom 0)
        active (atom true)
        conf (merge (read-storm-config) conf) ;; useful when testing
        storm-cluster-state (cluster/mk-storm-cluster-state conf)
        [event-manager cleanup-manager :as managers] [(event/event-manager false) (event/event-manager false)]
        inbox (master-inbox conf)
        storm-submit-lock (Object.)
        task-heartbeats-cache (atom {}) ; map from storm id -> task id -> {:nimbus-time :task-reported-time}
        downloaders (file-cache-map conf)
        uploaders (file-cache-map conf)
        uptime (uptime-computer)
        
        cleanup-fn (fn []
                     (let [id->kill-time (locking storm-submit-lock
                                           (topologies-to-kill storm-cluster-state))]
                       (when-not (empty? id->kill-time)
                         (log-message "Entering kill loop for " (pr-str id->kill-time)))
                       (loop [kill-order (sort-by second (seq id->kill-time))]
                         (when-not (empty? kill-order)
                           (let [[id kill-time] (first kill-order)
                                 now (current-time-secs)]
                             (if (or (nil? kill-time) (>= now kill-time))
                               (do
                                 ;; technically a supervisor could still think there's an assignment and try to d/l
                                 ;; this will cause supervisor to go down and come back up... eventually it should sync
                                 ;; TODO: removing code locally should be done separately (since topology that doesn't start will still have code)
                                 (log-message "Killing storm: " id)
                                 (rmr (master-stormdist-root conf id))
                                 (.remove-storm! storm-cluster-state id)
                                 (recur (rest kill-order))
                                 )
                               (do
                                 (log-message "Waiting for " (- kill-time now) " seconds to kill topology " id)
                                 (sleep-secs (- kill-time now))
                                 (recur kill-order)
                                 ))
                             )))
                       (when-not (empty? id->kill-time)
                         (log-message "Killed " (pr-str id->kill-time))))
                      (let [to-cleanup-ids (locking storm-submit-lock (cleanup-storm-ids conf storm-cluster-state))]
                        (when-not (empty? to-cleanup-ids)
                          (doseq [id to-cleanup-ids]
                            (.teardown-heartbeats! storm-cluster-state id)
                            (.teardown-task-errors! storm-cluster-state id)
                            (swap! task-heartbeats-cache dissoc id)
                            )
                          (log-message "Cleaned up topology task heartbeats: " (pr-str to-cleanup-ids))
                          )))
        reassign-fn (fn this []
                      (when (conf NIMBUS-REASSIGN)
                        (locking storm-submit-lock
                          (let [callback (fn [& ignored] (.add event-manager this))
                                active-storm-ids (.active-storms storm-cluster-state)]
                            (doseq [storm-id active-storm-ids]
                              (let [base (.storm-base storm-cluster-state storm-id nil)]
                                (if (= :active (-> base :status :type))
                                  (mk-assignments conf storm-id storm-cluster-state callback task-heartbeats-cache))))
                              ))))
        threads [(async-loop
                   (fn []
                     (.add event-manager reassign-fn)
                     (.add cleanup-manager cleanup-fn)
                     (when @active (conf NIMBUS-MONITOR-FREQ-SECS))
                     ))
                   ]]

    (reify Nimbus$Iface
      (^void submitTopology
             [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology]
             (when (storm-active? storm-cluster-state storm-name)
               (throw (AlreadyAliveException. storm-name)))
             (validate-topology! topology)
             (swap! submitted-count inc)
             (let [storm-id (str storm-name "-" @submitted-count "-" (current-time-secs))
                   storm-conf (from-json serializedConf)
                   storm-conf (assoc storm-conf STORM-ID storm-id)

                   total-storm-conf (merge conf storm-conf)
                   topology (if (total-storm-conf TOPOLOGY-OPTIMIZE) (optimize-topology topology) topology)]
               (log-message "Received topology submission for " storm-name " with conf " storm-conf)
               (setup-storm-code conf storm-id uploadedJarLocation storm-conf topology)
               ;; protects against multiple storms being submitted at once and cleanup thread killing storm in b/w
               ;; assignment and starting the storm
               (locking storm-submit-lock
                 (.setup-heartbeats! storm-cluster-state storm-id)
                 (setup-storm-static conf storm-id storm-cluster-state)
                 (mk-assignments conf storm-id storm-cluster-state (fn [& ignored] (.add event-manager reassign-fn)) task-heartbeats-cache)
                 (start-storm storm-name storm-cluster-state storm-id))
               ))
      
      (^void killTopology [this ^String name]
        (.killTopologyWithOpts this name (KillOptions.)))

      (^void killTopologyWithOpts [this ^String storm-name ^KillOptions options]
        (letlocals
         (bind storm-id
               (get-storm-id storm-cluster-state storm-name))         
         (when-not storm-id
           (throw (NotAliveException. storm-name)))
         (bind wait-amt
               (if (.is_set_wait_secs options)
                 (.get_wait_secs options)
                 ((read-storm-conf conf storm-id) TOPOLOGY-MESSAGE-TIMEOUT-SECS)
                 ))
         (locking storm-submit-lock
           (.update-storm! storm-cluster-state
                           storm-id
                           {:status {:type :killed
                                     :kill-time-secs (+ (current-time-secs) wait-amt)}}))
         (.add cleanup-manager cleanup-fn)
         (log-message "Deactivated " storm-name " and scheduled to be killed")
         ))

      (beginFileUpload [this]
        (let [fileloc (str inbox "/stormjar-" (uuid) ".jar")]
          (.put uploaders fileloc (Channels/newChannel (FileOutputStream. fileloc)))
          (log-message "Uploading file from client to " fileloc)
          fileloc
          ))
      
      (^void uploadChunk [this ^String location ^ByteBuffer chunk]
             (let [^WritableByteChannel channel (.get uploaders location)]
               (when-not channel
                 (throw (RuntimeException.
                         "File for that location does not exist (or timed out)")))
               (.write channel chunk)
               (.put uploaders location channel)
               ))

      (^void finishFileUpload [this ^String location]
             (let [^WritableByteChannel channel (.get uploaders location)]
               (when-not channel
                 (throw (RuntimeException.
                         "File for that location does not exist (or timed out)")))
               (.close channel)
               (log-message "Finished uploading file from client: " location)
               (.remove uploaders location)
               ))

      (^String beginFileDownload [this ^String file]
               (let [is (BufferFileInputStream. file)
                     id (uuid)]
                 (.put downloaders id is)
                 id
                 ))

      (^ByteBuffer downloadChunk [this ^String id]
              (let [^BufferFileInputStream is (.get downloaders id)]
                (when-not is
                  (throw (RuntimeException.
                          "Could not find input stream for that id")))
                (let [ret (.read is)]
                  (.put downloaders id is)
                  (when (empty? ret)
                    (.remove downloaders id))
                  (ByteBuffer/wrap ret)
                  )))

      (^String getTopologyConf [this ^String id]
               (to-json (read-storm-conf conf id)))

      (^StormTopology getTopology [this ^String id]
                      (read-storm-topology conf id))
      
      (^ClusterSummary getClusterInfo [this]
        (let [assigned (assigned-slots storm-cluster-state)
              supervisor-infos (all-supervisor-info storm-cluster-state)
              supervisor-summaries (dofor [[id info] supervisor-infos]
                                          (let [ports (set (:worker-ports info))
                                                ]
                                            (SupervisorSummary. (:hostname info)
                                                                (:uptime-secs info)
                                                                (count ports)
                                                                (count (assigned id)))
                                            ))
              nimbus-uptime (uptime)
              bases (topology-bases storm-cluster-state)
              topology-summaries (dofor [[id base] bases]
                                        (let [assignment (.assignment-info storm-cluster-state id nil)]
                                          (TopologySummary. id
                                                            (:storm-name base)
                                                            (-> (:task->node+port assignment)
                                                                keys
                                                                count)
                                                            (-> (:task->node+port assignment)
                                                                vals
                                                                set
                                                                count)
                                                            (time-delta (:launch-time-secs base))
                                                            (extract-status-str base)
                                                            )
                                          ))
              ]
          (ClusterSummary. supervisor-summaries
                           nimbus-uptime
                           topology-summaries)
          ))
      
      (^TopologyInfo getTopologyInfo [this ^String storm-id]
        (let [task-info (storm-task-info storm-cluster-state storm-id)
              base (.storm-base storm-cluster-state storm-id nil)
              assignment (.assignment-info storm-cluster-state storm-id nil)
              task-summaries (dofor [[task component] task-info]
                                    (let [[node port] (get-in assignment [:task->node+port task])
                                          host (-> assignment :node->host (get node))
                                          heartbeat (.task-heartbeat storm-cluster-state storm-id task)
                                          errors (.task-errors storm-cluster-state storm-id task)
                                          errors (dofor [e errors] (ErrorInfo. (:error e) (:time-secs e)))
                                          stats (:stats heartbeat)
                                          stats (if stats
                                                  (stats/thriftify-task-stats stats))]
                                      (doto
                                          (TaskSummary. task
                                                        component
                                                        host
                                                        port
                                                        (nil-to-zero
                                                         (:uptime-secs heartbeat))
                                                        errors
                                                        )
                                        (.set_stats stats))
                                      ))
              ]
          (TopologyInfo. storm-id
                         (:storm-name base)
                         (time-delta (:launch-time-secs base))
                         task-summaries
                         (extract-status-str base)
                         )
          ))
      
      Shutdownable
        (shutdown [this]
          (log-message "Shutting down master")
          (reset! active false)
          (doseq [t threads]
                 (.interrupt t)
                 (.join t))
          (.shutdown event-manager)
          (.shutdown cleanup-manager)
          (.disconnect storm-cluster-state)
          (log-message "Shut down master")
          )
     DaemonCommon
       (waiting? [this]
         (and
          (every? (memfn sleeping?) threads)
          (every? (memfn waiting?) managers)
          )))))

(defn launch-server! [conf]
  (validate-distributed-mode! conf)
  (let [service-handler (service-handler conf)
        options (-> (TNonblockingServerSocket. (int (conf NIMBUS-THRIFT-PORT)))
                    (THsHaServer$Args.)
                    (.workerThreads 64)
                    (.protocolFactory (TBinaryProtocol$Factory.))
                    (.processor (Nimbus$Processor. service-handler))
                    )
       server (THsHaServer. options)]
    (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.shutdown service-handler) (.stop server))))
    (log-message "Starting Nimbus server...")
    (.serve server)))


;; distributed implementation

(defmethod setup-jar :distributed [conf tmp-jar-location stormroot]
           (let [src-file (File. tmp-jar-location)]
             (if-not (.exists src-file)
               (throw
                (IllegalArgumentException.
                 (str tmp-jar-location " to copy to " stormroot " does not exist!"))))
             (FileUtils/copyFile src-file (File. (master-stormjar-path stormroot)))
             ))

;; local implementation

(defmethod setup-jar :local [conf & args]
  nil
  )


(defn -main []
  (launch-server! (read-storm-config)))
