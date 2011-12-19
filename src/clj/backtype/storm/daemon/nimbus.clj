(ns backtype.storm.daemon.nimbus
  (:import [org.apache.thrift7.server THsHaServer THsHaServer$Args])
  (:import [org.apache.thrift7.protocol TBinaryProtocol TBinaryProtocol$Factory])
  (:import [org.apache.thrift7 TException])
  (:import [org.apache.thrift7.transport TNonblockingServerTransport TNonblockingServerSocket])
  (:import [java.nio ByteBuffer])
  (:import [java.nio.channels Channels WritableByteChannel])
  (:use [backtype.storm bootstrap])
  (:use [backtype.storm.daemon common])
  (:use [clojure.contrib.def :only [defnk]])
  (:gen-class))

(bootstrap)


(defn file-cache-map [conf]
  (TimeCacheMap.
   (int (conf NIMBUS-FILE-COPY-EXPIRATION-SECS))
   (reify TimeCacheMap$ExpiredCallback
          (expire [this id stream]
                  (.close stream)
                  ))
   ))

(defn nimbus-data [conf]
  {:conf conf
   :submitted-count (atom 0)
   :storm-cluster-state (cluster/mk-storm-cluster-state conf)
   :submit-lock (Object.)
   :task-heartbeats-cache (atom {})
   :downloaders (file-cache-map conf)
   :uploaders (file-cache-map conf)
   :uptime (uptime-computer)
   :timer (mk-timer :kill-fn (fn [t]
                               (log-error t "Error when processing event")
                               (halt-process! 20 "Error when processing an event")
                               ))
   })

(defn inbox [nimbus]
  (master-inbox (:conf nimbus)))

(defn- read-storm-conf [conf storm-id]
  (let [stormroot (master-stormdist-root conf storm-id)]
    (merge conf
           (Utils/deserialize
            (FileUtils/readFileToByteArray
             (File. (master-stormconf-path stormroot))
             )))))

(defn set-topology-status! [nimbus storm-id status]
  (let [storm-cluster-state (:storm-cluster-state nimbus)]
   (.update-storm! storm-cluster-state
                   storm-id
                   {:status status})
   (log-message "Updated " storm-id " with status " status)
   ))

(declare reassign-topology)
(declare delay-event)
(declare mk-assignments)

(defn kill-transition [nimbus storm-id]
  (fn [kill-time]
    (let [delay (if kill-time
                  kill-time
                  (get (read-storm-conf (:conf nimbus) storm-id)
                       TOPOLOGY-MESSAGE-TIMEOUT-SECS))]
      (delay-event nimbus
                   storm-id
                   delay
                   :remove)
      {:type :killed
       :kill-time-secs delay})
    ))

(defn rebalance-transition [nimbus storm-id status]
  (fn [time]
    (let [delay (if time
                  time
                  (get (read-storm-conf (:conf nimbus) storm-id)
                       TOPOLOGY-MESSAGE-TIMEOUT-SECS))]
      (delay-event nimbus
                   storm-id
                   delay
                   :do-rebalance)
      {:type :rebalancing
       :delay-secs delay
       :old-status status
       })))

(defn reassign-transition [nimbus storm-id]
  (fn []
    (reassign-topology nimbus storm-id)
    nil
    ))

;; there's a minor problem where rebalancing is scheduled over and over until it finally happens
;; can fix this by either:
;; 1. detecting whether it's scheduled or not...
;; 2. not using monitor event, but rather some sort of "on startup" event
;; 3. generating a "rebalance id" and only rebalancing if current status has that id

(defn state-transitions [nimbus storm-id status]
  {:active {:monitor (reassign-transition nimbus storm-id)
            :inactivate :inactive            
            :activate nil
            :rebalance (rebalance-transition nimbus storm-id status)
            :kill (kill-transition nimbus storm-id)
            }
   :inactive {:monitor (reassign-transition nimbus storm-id)
              :activate :active
              :inactivate nil
              :rebalance (rebalance-transition nimbus storm-id status)
              :kill (kill-transition nimbus storm-id)
              }
   :killed {:monitor (fn [] (delay-event nimbus
                                         storm-id
                                         (:kill-time-secs status)
                                         :remove))
            :kill (kill-transition nimbus storm-id)
            :remove (fn []
                      (log-message "Killing topology: " storm-id)
                      (.remove-storm! (:storm-cluster-state nimbus)
                                      storm-id)
                      nil)
            }
   :rebalancing {:monitor (fn [] (delay-event nimbus
                                              storm-id
                                              (:delay-secs status)
                                              :do-rebalance))
                 :kill (kill-transition nimbus storm-id)
                 :do-rebalance (fn []
                                 (mk-assignments nimbus storm-id :scratch? true)
                                 (:old-status status))
                 }})

(defn topology-status [nimbus storm-id]
  (-> nimbus :storm-cluster-state (.storm-base storm-id nil) :status))

(defn transition!
  ([nimbus storm-id event]
     (transition! nimbus storm-id event false))
  ([nimbus storm-id event error-on-no-transition?]
     (locking (:submit-lock nimbus)
       (let [[event & event-args] (if (keyword? event) [event] event)
             status (topology-status nimbus storm-id)]
         ;; handles the case where event was scheduled but topology has been removed
         (if-not status
           (log-message "Cannot apply event " event " to " storm-id " because topology no longer exists")
           (let [get-event (fn [m e]
                             (if (contains? m e)
                               (m e)
                               (let [msg (str "No transition for event: " event
                                              ", status: " status,
                                              " storm-id: " storm-id)]
                                 (if error-on-no-transition?
                                   (throw-runtime msg)
                                   (do (log-message msg) nil)
                                   ))))
                 transition (-> (state-transitions nimbus storm-id status)
                                (get (:type status))
                                (get-event event))
                 transition (if (or (nil? transition)
                                    (keyword? transition))
                              (fn [] transition)
                              transition)
                 new-status (apply transition event-args)
                 new-status (if (keyword? new-status)
                              {:type new-status}
                              new-status)]
             (when new-status
               (set-topology-status! nimbus storm-id new-status)))))
       )))

(defn transition-name! [nimbus storm-name event & args]
  (let [storm-id (get-storm-id (:storm-cluster-state nimbus) storm-name)]
    (when-not storm-id
      (throw (NotAliveException. storm-name)))
    (apply transition! nimbus storm-id event args)))

(defn delay-event [nimbus storm-id delay-secs event]
  (log-message "Delaying event " event " for " delay-secs " secs for " storm-id)
  (schedule (:timer nimbus)
            delay-secs
            #(transition! nimbus storm-id event false)
            ))

;; active -> reassign in X secs

;; killed -> wait kill time then shutdown
;; active -> reassign in X secs
;; inactive -> nothing
;; rebalance -> wait X seconds then rebalance
;; swap... (need to handle kill during swap, etc.)
;; event transitions are delayed by timer... anything else that comes through (e.g. a kill) override the transition? or just disable other transitions during the transition?


(defmulti setup-jar cluster-mode)

;; status types
;; -- killed (:kill-time-secs)
;; -- active
;; -- inactive
;; -- swapping (:name, :launch-wait-time [defaults to launch timeout] :inactive-wait-time[ message timeout for active topology]) --> steps: wait launch timeout, inactivate other topology, wait message timeout, kill other topology (with timeout of 0), activate swapped topology
;;  State transitions:
;;    -- swapped + active other = wait + inactivate other
;;    -- inactive other + swapped = wait message timeout + kill(0)
;;    -- swapped + killed other = activate
;; -- rebalance, :wait-time
;;      -- after waiting, should compute new assignment from scratch

;; swapping design
;; -- need 2 ports per worker (swap port and regular port)
;; -- topology that swaps in can use all the existing topologies swap ports, + unused worker slots
;; -- how to define worker resources? port range + number of workers?


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

(defn get-node->host [storm-cluster-state callback]
  (->> (all-supervisor-info storm-cluster-state callback)
       (map-val :hostname)))

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
    (mapcat
     (fn [[id slots]]
       (for [s (set/difference slots (existing-slots id))]
         [id s]))
     all-slots)
    ))

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


(defn- read-storm-topology [conf storm-id]
  (let [stormroot (master-stormdist-root conf storm-id)]
    (Utils/deserialize
      (FileUtils/readFileToByteArray
        (File. (master-stormcode-path stormroot))
        ))))

;; public so it can be mocked in tests
(defn mk-task-component-assignments [conf storm-id]
  (let [storm-conf (read-storm-conf conf storm-id)
        max-parallelism (storm-conf TOPOLOGY-MAX-TASK-PARALLELISM)
        topology (system-topology storm-conf (read-storm-topology conf storm-id))
        slots-to-use (storm-conf TOPOLOGY-WORKERS)
        counter (mk-counter)
        tasks (concat
               (mapcat (mk-task-maker max-parallelism bolt-parallelism counter)
                       (.get_bolts topology))
               (mapcat (mk-task-maker max-parallelism spout-parallelism counter)
                       (.get_spouts topology))
               (mapcat (mk-task-maker max-parallelism state-spout-parallelism counter)
                       (.get_state_spouts topology))
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
(defn compute-new-task->node+port [conf storm-id existing-assignment storm-cluster-state callback task-heartbeats-cache scratch?]
  (let [available-slots (available-slots conf storm-cluster-state callback)        
        storm-conf (read-storm-conf conf storm-id)
        all-task-ids (set (.task-ids storm-cluster-state storm-id))

        existing-assigned (reverse-map (:task->node+port existing-assignment))
        alive-ids (if scratch?
                    all-task-ids
                    (set (alive-tasks conf storm-id storm-cluster-state
                                      all-task-ids (:task->start-time-secs existing-assignment)
                                      task-heartbeats-cache)))
        
        alive-assigned (filter-val (partial every? alive-ids) existing-assigned)

        total-slots-to-use (min (storm-conf TOPOLOGY-WORKERS)
                                (+ (count available-slots) (count alive-assigned)))
        keep-assigned (if scratch?
                        {}
                        (keeper-slots alive-assigned (count all-task-ids) total-slots-to-use))
        
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
(defnk mk-assignments [nimbus storm-id :scratch? false]
  (log-debug "Determining assignment for " storm-id)
  (let [conf (:conf nimbus)
        storm-cluster-state (:storm-cluster-state nimbus)
        callback (fn [& ignored] (transition! nimbus storm-id :monitor))
        node->host (get-node->host storm-cluster-state callback)

        existing-assignment (.assignment-info storm-cluster-state storm-id nil)
        task->node+port (compute-new-task->node+port conf storm-id existing-assignment
                                                     storm-cluster-state callback
                                                     (:task-heartbeats-cache nimbus)
                                                     scratch?)
        
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

(defn reassign-topology [nimbus storm-id]
  (let [conf (:conf nimbus)
        storm-cluster-state (:storm-cluster-state nimbus)]
    (when (conf NIMBUS-REASSIGN)      
      (mk-assignments nimbus
                      storm-id))))

(defn- start-storm [storm-name storm-cluster-state storm-id]
  (log-message "Activating " storm-name ": " storm-id)
  (.activate-storm! storm-cluster-state
                    storm-id
                    (StormBase. storm-name
                                (current-time-secs)
                                {:type :active})))

;; Master:
;; job submit:
;; 1. read which nodes are available
;; 2. set up the worker/{storm}/{task} stuff (static)
;; 3. set assignments
;; 4. start storm - necessary in case master goes down, when goes back up can remember to take down the storm (2 states: on or off)

(defn storm-active? [storm-cluster-state storm-name]
  (not-nil? (get-storm-id storm-cluster-state storm-name)))

(defn check-storm-active! [nimbus storm-name active?]
  (if (= (not active?)
         (storm-active? (:storm-cluster-state nimbus)
                        storm-name))
    (if active?
      (throw (NotAliveException. (str storm-name " is not alive")))
      (throw (AlreadyAliveException. (str storm-name " is already active"))))
    ))

(defn code-ids [conf]
  (-> conf
      master-stormdist-root
      read-dir-contents
      set
      ))

(defn cleanup-storm-ids [conf storm-cluster-state]
  (let [heartbeat-ids (set (.heartbeat-storms storm-cluster-state))
        error-ids (set (.task-error-storms storm-cluster-state))
        code-ids (code-ids conf)
        assigned-ids (set (.active-storms storm-cluster-state))]
    (set/difference (set/union heartbeat-ids error-ids code-ids) assigned-ids)
    ))

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
    (when-not (every? (complement system-component?) (concat bolt-ids spout-ids state-spout-ids))
      (throw
       (InvalidTopologyException.
        "Component ids cannot start with '__'")))
    ;; TODO: validate that every declared stream is not a system stream
    ))

(defn extract-status-str [base]
  (let [t (-> base :status :type)]
    (.toUpperCase (name t))
    ))

(defn mapify-serializations [sers]
  (->> sers
       (map (fn [e] (if (map? e) e {e nil})))
       (apply merge)
       ))

(defn normalize-conf [conf storm-conf]
  ;; ensure that serializations are same for all tasks no matter what's on
  ;; the supervisors. this also allows you to declare the serializations as a sequence
  (let [sers (storm-conf TOPOLOGY-KRYO-REGISTER)
        sers (if sers sers (conf TOPOLOGY-KRYO-REGISTER))
        sers (mapify-serializations sers)]
    (assoc storm-conf TOPOLOGY-KRYO-REGISTER sers)
    ))

(defn do-cleanup [nimbus]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        conf (:conf nimbus)
        submit-lock (:submit-lock nimbus)]
    (let [to-cleanup-ids (locking submit-lock
                           (cleanup-storm-ids conf storm-cluster-state))]
      (when-not (empty? to-cleanup-ids)
        (doseq [id to-cleanup-ids]
          (log-message "Cleaning up " id)
          (.teardown-heartbeats! storm-cluster-state id)
          (.teardown-task-errors! storm-cluster-state id)
          (rmr (master-stormdist-root conf id))
          (swap! (:task-heartbeats-cache nimbus) dissoc id))
        ))))


(defserverfn service-handler [conf]
  (log-message "Starting Nimbus with conf " conf)
  (let [nimbus (nimbus-data conf)]
    (schedule-recurring (:timer nimbus)
                        0
                        (conf NIMBUS-MONITOR-FREQ-SECS)
                        (fn []
                          (doseq [storm-id (.active-storms
                                            (:storm-cluster-state nimbus))]
                            (transition! nimbus storm-id :monitor))
                          (do-cleanup nimbus)
                          ))
    (reify Nimbus$Iface
      (^void submitTopology
        [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology]
        (check-storm-active! nimbus storm-name false)
        (validate-topology! topology)
        (swap! (:submitted-count nimbus) inc)
        (let [storm-id (str storm-name "-" @(:submitted-count nimbus) "-" (current-time-secs))
              storm-conf (normalize-conf
                          conf
                          (-> serializedConf
                              from-json
                              (assoc STORM-ID storm-id)
                              ))
              total-storm-conf (merge conf storm-conf)
              topology (if (total-storm-conf TOPOLOGY-OPTIMIZE)
                         (optimize-topology topology)
                         topology)
              storm-cluster-state (:storm-cluster-state nimbus)]
          (log-message "Received topology submission for " storm-name " with conf " storm-conf)
          ;; lock protects against multiple topologies being submitted at once and
          ;; cleanup thread killing topology in b/w assignment and starting the topology
          (locking (:submit-lock nimbus)
            (setup-storm-code conf storm-id uploadedJarLocation storm-conf topology)
            (.setup-heartbeats! storm-cluster-state storm-id)
            (setup-storm-static conf storm-id storm-cluster-state)
            (mk-assignments nimbus storm-id)
            (start-storm storm-name storm-cluster-state storm-id))
          ))
      
      (^void killTopology [this ^String name]
        (.killTopologyWithOpts this name (KillOptions.)))

      (^void killTopologyWithOpts [this ^String storm-name ^KillOptions options]
        (check-storm-active! nimbus storm-name true)
        (let [wait-amt (if (.is_set_wait_secs options)
                         (.get_wait_secs options)                         
                         )]
          (transition-name! nimbus storm-name [:kill wait-amt] true)
          ))

      (^void rebalance [this ^String storm-name ^RebalanceOptions options]
        (check-storm-active! nimbus storm-name true)
        (let [wait-amt (if (.is_set_wait_secs options)
                         (.get_wait_secs options)                         
                         )]
          (transition-name! nimbus storm-name [:rebalance wait-amt] true)
          ))      

      (activate [this storm-name]
        (transition-name! nimbus storm-name :activate true)
        )

      (deactivate [this storm-name]
        (transition-name! nimbus storm-name :inactivate true))

      (beginFileUpload [this]
        (let [fileloc (str (inbox nimbus) "/stormjar-" (uuid) ".jar")]
          (.put (:uploaders nimbus)
                fileloc
                (Channels/newChannel (FileOutputStream. fileloc)))
          (log-message "Uploading file from client to " fileloc)
          fileloc
          ))

      (^void uploadChunk [this ^String location ^ByteBuffer chunk]
        (let [uploaders (:uploaders nimbus)
              ^WritableByteChannel channel (.get uploaders location)]
          (when-not channel
            (throw (RuntimeException.
                    "File for that location does not exist (or timed out)")))
          (.write channel chunk)
          (.put uploaders location channel)
          ))

      (^void finishFileUpload [this ^String location]
        (let [uploaders (:uploaders nimbus)
              ^WritableByteChannel channel (.get uploaders location)]
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
          (.put (:downloaders nimbus) id is)
          id
          ))

      (^ByteBuffer downloadChunk [this ^String id]
        (let [downloaders (:downloaders nimbus)
              ^BufferFileInputStream is (.get downloaders id)]
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
        (system-topology (read-storm-conf conf id) (read-storm-topology conf id)))

      (^ClusterSummary getClusterInfo [this]
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              assigned (assigned-slots storm-cluster-state)
              supervisor-infos (all-supervisor-info storm-cluster-state)
              supervisor-summaries (dofor [[id info] supervisor-infos]
                                          (let [ports (set (:worker-ports info))
                                                ]
                                            (SupervisorSummary. (:hostname info)
                                                                (:uptime-secs info)
                                                                (count ports)
                                                                (count (assigned id)))
                                            ))
              nimbus-uptime ((:uptime nimbus))
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
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              task-info (storm-task-info storm-cluster-state storm-id)
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
        (cancel-timer (:timer nimbus))
        (.disconnect (:storm-cluster-state nimbus))
        (log-message "Shut down master")
        )
      DaemonCommon
      (waiting? [this]
        (timer-waiting? (:timer nimbus))))))

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
