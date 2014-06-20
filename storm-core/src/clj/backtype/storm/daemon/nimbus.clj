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
(ns backtype.storm.daemon.nimbus
  (:import [org.apache.thrift.server THsHaServer THsHaServer$Args])
  (:import [org.apache.thrift.protocol TBinaryProtocol TBinaryProtocol$Factory])
  (:import [org.apache.thrift.exception])
  (:import [org.apache.thrift.transport TNonblockingServerTransport TNonblockingServerSocket])
  (:import [java.nio ByteBuffer])
  (:import [java.io FileNotFoundException])
  (:import [java.nio.channels Channels WritableByteChannel])
  (:use [backtype.storm.scheduler.DefaultScheduler])
  (:import [backtype.storm.scheduler INimbus SupervisorDetails WorkerSlot TopologyDetails
            Cluster Topologies SchedulerAssignment SchedulerAssignmentImpl DefaultScheduler ExecutorDetails])
  (:use [backtype.storm bootstrap util])
  (:use [backtype.storm.config :only [validate-configs-with-schemas]])
  (:use [backtype.storm.daemon common])
  (:gen-class
    :methods [^{:static true} [launch [backtype.storm.scheduler.INimbus] void]]))

(bootstrap)

(defn file-cache-map [conf]
  (TimeCacheMap.
   (int (conf NIMBUS-FILE-COPY-EXPIRATION-SECS))
   (reify TimeCacheMap$ExpiredCallback
          (expire [this id stream]
                  (.close stream)
                  ))
   ))

(defn mk-scheduler [conf inimbus]
  (let [forced-scheduler (.getForcedScheduler inimbus)
        scheduler (cond
                    forced-scheduler
                    (do (log-message "Using forced scheduler from INimbus " (class forced-scheduler))
                        forced-scheduler)
    
                    (conf STORM-SCHEDULER)
                    (do (log-message "Using custom scheduler: " (conf STORM-SCHEDULER))
                        (-> (conf STORM-SCHEDULER) new-instance))
    
                    :else
                    (do (log-message "Using default scheduler")
                        (DefaultScheduler.)))]
    (.prepare scheduler conf)
    scheduler
    ))

(defn nimbus-data [conf inimbus]
  (let [forced-scheduler (.getForcedScheduler inimbus)]
    {:conf conf
     :inimbus inimbus
     :submitted-count (atom 0)
     :storm-cluster-state (cluster/mk-storm-cluster-state conf)
     :submit-lock (Object.)
     :heartbeats-cache (atom {})
     :downloaders (file-cache-map conf)
     :uploaders (file-cache-map conf)
     :uptime (uptime-computer)
     :validator (new-instance (conf NIMBUS-TOPOLOGY-VALIDATOR))
     :timer (mk-timer :kill-fn (fn [t]
                                 (log-error t "Error when processing event")
                                 (halt-process! 20 "Error when processing an event")
                                 ))
     :scheduler (mk-scheduler conf inimbus)
     }))

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
  (fn [time num-workers executor-overrides]
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
       :num-workers num-workers
       :executor-overrides executor-overrides
       })))

(defn do-rebalance [nimbus storm-id status]
  (.update-storm! (:storm-cluster-state nimbus)
                  storm-id
                  (assoc-non-nil
                    {:component->executors (:executor-overrides status)}
                    :num-workers
                    (:num-workers status)))
  (mk-assignments nimbus :scratch-topology-id storm-id))

(defn state-transitions [nimbus storm-id status]
  {:active {:inactivate :inactive            
            :activate nil
            :rebalance (rebalance-transition nimbus storm-id status)
            :kill (kill-transition nimbus storm-id)
            }
   :inactive {:activate :active
              :inactivate nil
              :rebalance (rebalance-transition nimbus storm-id status)
              :kill (kill-transition nimbus storm-id)
              }
   :killed {:startup (fn [] (delay-event nimbus
                                         storm-id
                                         (:kill-time-secs status)
                                         :remove)
                             nil)
            :kill (kill-transition nimbus storm-id)
            :remove (fn []
                      (log-message "Killing topology: " storm-id)
                      (.remove-storm! (:storm-cluster-state nimbus)
                                      storm-id)
                      nil)
            }
   :rebalancing {:startup (fn [] (delay-event nimbus
                                              storm-id
                                              (:delay-secs status)
                                              :do-rebalance)
                                 nil)
                 :kill (kill-transition nimbus storm-id)
                 :do-rebalance (fn []
                                 (do-rebalance nimbus storm-id status)
                                 (:old-status status))
                 }})

(defn topology-status [nimbus storm-id]
  (-> nimbus :storm-cluster-state (.storm-base storm-id nil) :status))

(defn transition!
  ([nimbus storm-id event]
     (transition! nimbus storm-id event false))
  ([nimbus storm-id event error-on-no-transition?]
     (locking (:submit-lock nimbus)
       (let [system-events #{:startup}
             [event & event-args] (if (keyword? event) [event] event)
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
                                   (do (when-not (contains? system-events event)
                                         (log-message msg))
                                       nil))
                                 )))
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
(defmulti clean-inbox cluster-mode)

;; swapping design
;; -- need 2 ports per worker (swap port and regular port)
;; -- topology that swaps in can use all the existing topologies swap ports, + unused worker slots
;; -- how to define worker resources? port range + number of workers?


;; Monitoring (or by checking when nodes go down or heartbeats aren't received):
;; 1. read assignment
;; 2. see which executors/nodes are up
;; 3. make new assignment to fix any problems
;; 4. if a storm exists but is not taken down fully, ensure that storm takedown is launched (step by step remove executors and finally remove assignments)

(defn- assigned-slots
  "Returns a map from node-id to a set of ports"
  [storm-cluster-state]
  (let [assignments (.assignments storm-cluster-state nil)
        ]
    (defaulted
      (apply merge-with set/union
             (for [a assignments
                   [_ [node port]] (-> (.assignment-info storm-cluster-state a nil) :executor->node+port)]
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

(defn- all-scheduling-slots
  [nimbus topologies missing-assignment-topologies]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        ^INimbus inimbus (:inimbus nimbus)
        
        supervisor-infos (all-supervisor-info storm-cluster-state nil)

        supervisor-details (dofor [[id info] supervisor-infos]
                             (SupervisorDetails. id (:meta info)))

        ret (.allSlotsAvailableForScheduling inimbus
                     supervisor-details
                     topologies
                     (set missing-assignment-topologies)
                     )
        ]
    (dofor [^WorkerSlot slot ret]
      [(.getNodeId slot) (.getPort slot)]
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

(declare compute-executor->component)

(defn read-topology-details [nimbus storm-id]
  (let [conf (:conf nimbus)
        storm-base (.storm-base (:storm-cluster-state nimbus) storm-id nil)
        topology-conf (read-storm-conf conf storm-id)
        topology (read-storm-topology conf storm-id)
        executor->component (->> (compute-executor->component nimbus storm-id)
                                 (map-key (fn [[start-task end-task]]
                                            (ExecutorDetails. (int start-task) (int end-task)))))]
    (TopologyDetails. storm-id
                      topology-conf
                      topology
                      (:num-workers storm-base)
                      executor->component
                      )))

;; Does not assume that clocks are synchronized. Executor heartbeat is only used so that
;; nimbus knows when it's received a new heartbeat. All timing is done by nimbus and
;; tracked through heartbeat-cache
(defn- update-executor-cache [curr hb timeout]
  (let [reported-time (:time-secs hb)
        {last-nimbus-time :nimbus-time
         last-reported-time :executor-reported-time} curr
        reported-time (cond reported-time reported-time
                            last-reported-time last-reported-time
                            :else 0)
        nimbus-time (if (or (not last-nimbus-time)
                        (not= last-reported-time reported-time))
                      (current-time-secs)
                      last-nimbus-time
                      )]
      {:is-timed-out (and
                       nimbus-time
                       (>= (time-delta nimbus-time) timeout))
       :nimbus-time nimbus-time
       :executor-reported-time reported-time}))

(defn update-heartbeat-cache [cache executor-beats all-executors timeout]
  (let [cache (select-keys cache all-executors)]
    (into {}
      (for [executor all-executors :let [curr (cache executor)]]
        [executor
         (update-executor-cache curr (get executor-beats executor) timeout)]
         ))))

(defn update-heartbeats! [nimbus storm-id all-executors existing-assignment]
  (log-debug "Updating heartbeats for " storm-id " " (pr-str all-executors))
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        executor-beats (.executor-beats storm-cluster-state storm-id (:executor->node+port existing-assignment))
        cache (update-heartbeat-cache (@(:heartbeats-cache nimbus) storm-id)
                                      executor-beats
                                      all-executors
                                      ((:conf nimbus) NIMBUS-TASK-TIMEOUT-SECS))]
      (swap! (:heartbeats-cache nimbus) assoc storm-id cache)))

(defn- update-all-heartbeats! [nimbus existing-assignments topology->executors]
  "update all the heartbeats for all the topologies's executors"
  (doseq [[tid assignment] existing-assignments
          :let [all-executors (topology->executors tid)]]
    (update-heartbeats! nimbus tid all-executors assignment)))

(defn- alive-executors
  [nimbus ^TopologyDetails topology-details all-executors existing-assignment]
  (log-debug "Computing alive executors for " (.getId topology-details) "\n"
             "Executors: " (pr-str all-executors) "\n"
             "Assignment: " (pr-str existing-assignment) "\n"
             "Heartbeat cache: " (pr-str (@(:heartbeats-cache nimbus) (.getId topology-details)))
             )
  ;; TODO: need to consider all executors associated with a dead executor (in same slot) dead as well,
  ;; don't just rely on heartbeat being the same
  (let [conf (:conf nimbus)
        storm-id (.getId topology-details)
        executor-start-times (:executor->start-time-secs existing-assignment)
        heartbeats-cache (@(:heartbeats-cache nimbus) storm-id)]
    (->> all-executors
        (filter (fn [executor]
          (let [start-time (get executor-start-times executor)
                is-timed-out (-> heartbeats-cache (get executor) :is-timed-out)]
            (if (and start-time
                   (or
                    (< (time-delta start-time)
                       (conf NIMBUS-TASK-LAUNCH-SECS))
                    (not is-timed-out)
                    ))
              true
              (do
                (log-message "Executor " storm-id ":" executor " not alive")
                false))
            )))
        doall)))


(defn- to-executor-id [task-ids]
  [(first task-ids) (last task-ids)])

(defn- compute-executors [nimbus storm-id]
  (let [conf (:conf nimbus)
        storm-base (.storm-base (:storm-cluster-state nimbus) storm-id nil)
        component->executors (:component->executors storm-base)
        storm-conf (read-storm-conf conf storm-id)
        topology (read-storm-topology conf storm-id)
        task->component (storm-task-info topology storm-conf)]
    (->> (storm-task-info topology storm-conf)
         reverse-map
         (map-val sort)
         (join-maps component->executors)
         (map-val (partial apply partition-fixed))
         (mapcat second)
         (map to-executor-id)
         )))

(defn- compute-executor->component [nimbus storm-id]
  (let [conf (:conf nimbus)
        executors (compute-executors nimbus storm-id)
        topology (read-storm-topology conf storm-id)
        storm-conf (read-storm-conf conf storm-id)
        task->component (storm-task-info topology storm-conf)
        executor->component (into {} (for [executor executors
                                           :let [start-task (first executor)
                                                 component (task->component start-task)]]
                                       {executor component}))]
        executor->component))

(defn- compute-topology->executors [nimbus storm-ids]
  "compute a topology-id -> executors map"
  (into {} (for [tid storm-ids]
             {tid (set (compute-executors nimbus tid))})))

(defn- compute-topology->alive-executors [nimbus existing-assignments topologies topology->executors scratch-topology-id]
  "compute a topology-id -> alive executors map"
  (into {} (for [[tid assignment] existing-assignments
                 :let [topology-details (.getById topologies tid)
                       all-executors (topology->executors tid)
                       alive-executors (if (and scratch-topology-id (= scratch-topology-id tid))
                                         all-executors
                                         (set (alive-executors nimbus topology-details all-executors assignment)))]]
             {tid alive-executors})))
  
(defn- compute-supervisor->dead-ports [nimbus existing-assignments topology->executors topology->alive-executors]
  (let [dead-slots (into [] (for [[tid assignment] existing-assignments
                                  :let [all-executors (topology->executors tid)
                                        alive-executors (topology->alive-executors tid)
                                        dead-executors (set/difference all-executors alive-executors)
                                        dead-slots (->> (:executor->node+port assignment)
                                                        (filter #(contains? dead-executors (first %)))
                                                        vals)]]
                              dead-slots))
        supervisor->dead-ports (->> dead-slots
                                    (apply concat)
                                    (map (fn [[sid port]] {sid #{port}}))
                                    (apply (partial merge-with set/union)))]
    (or supervisor->dead-ports {})))

(defn- compute-topology->scheduler-assignment [nimbus existing-assignments topology->alive-executors]
  "convert assignment information in zk to SchedulerAssignment, so it can be used by scheduler api."
  (into {} (for [[tid assignment] existing-assignments
                 :let [alive-executors (topology->alive-executors tid)
                       executor->node+port (:executor->node+port assignment)
                       executor->slot (into {} (for [[executor [node port]] executor->node+port]
                                                 ;; filter out the dead executors
                                                 (if (contains? alive-executors executor)
                                                   {(ExecutorDetails. (first executor)
                                                                      (second executor))
                                                    (WorkerSlot. node port)}
                                                   {})))]]
             {tid (SchedulerAssignmentImpl. tid executor->slot)})))

(defn- read-all-supervisor-details [nimbus all-scheduling-slots supervisor->dead-ports]
  "return a map: {topology-id SupervisorDetails}"
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        supervisor-infos (all-supervisor-info storm-cluster-state)
        nonexistent-supervisor-slots (apply dissoc all-scheduling-slots (keys supervisor-infos))
        all-supervisor-details (into {} (for [[sid supervisor-info] supervisor-infos
                                              :let [hostname (:hostname supervisor-info)
                                                    scheduler-meta (:scheduler-meta supervisor-info)
                                                    dead-ports (supervisor->dead-ports sid)
                                                    ;; hide the dead-ports from the all-ports
                                                    ;; these dead-ports can be reused in next round of assignments
                                                    all-ports (-> (get all-scheduling-slots sid)
                                                                  (set/difference dead-ports)
                                                                  ((fn [ports] (map int ports))))
                                                    supervisor-details (SupervisorDetails. sid hostname scheduler-meta all-ports)]]
                                          {sid supervisor-details}))]
    (merge all-supervisor-details 
           (into {}
              (for [[sid ports] nonexistent-supervisor-slots]
                [sid (SupervisorDetails. sid nil ports)]))
           )))

(defn- compute-topology->executor->node+port [scheduler-assignments]
  "convert {topology-id -> SchedulerAssignment} to
           {topology-id -> {executor [node port]}}"
  (map-val (fn [^SchedulerAssignment assignment]
             (->> assignment
                  .getExecutorToSlot
                  (#(into {} (for [[^ExecutorDetails executor ^WorkerSlot slot] %]
                              {[(.getStartTask executor) (.getEndTask executor)]
                               [(.getNodeId slot) (.getPort slot)]})))))
           scheduler-assignments))

;; NEW NOTES
;; only assign to supervisors who are there and haven't timed out
;; need to reassign workers with executors that have timed out (will this make it brittle?)
;; need to read in the topology and storm-conf from disk
;; if no slots available and no slots used by this storm, just skip and do nothing
;; otherwise, package rest of executors into available slots (up to how much it needs)

;; in the future could allocate executors intelligently (so that "close" tasks reside on same machine)

;; TODO: slots that have dead executor should be reused as long as supervisor is active


;; (defn- assigned-slots-from-scheduler-assignments [topology->assignment]
;;   (->> topology->assignment
;;        vals
;;        (map (fn [^SchedulerAssignment a] (.getExecutorToSlot a)))
;;        (mapcat vals)
;;        (map (fn [^WorkerSlot s] {(.getNodeId s) #{(.getPort s)}}))
;;        (apply merge-with set/union)
;;        ))

(defn num-used-workers [^SchedulerAssignment scheduler-assignment]
  (if scheduler-assignment
    (count (.getSlots scheduler-assignment))
    0 ))

;; public so it can be mocked out
(defn compute-new-topology->executor->node+port [nimbus existing-assignments topologies scratch-topology-id]
  (let [conf (:conf nimbus)
        storm-cluster-state (:storm-cluster-state nimbus)
        topology->executors (compute-topology->executors nimbus (keys existing-assignments))
        ;; update the executors heartbeats first.
        _ (update-all-heartbeats! nimbus existing-assignments topology->executors)
        topology->alive-executors (compute-topology->alive-executors nimbus
                                                                     existing-assignments
                                                                     topologies
                                                                     topology->executors
                                                                     scratch-topology-id)
        supervisor->dead-ports (compute-supervisor->dead-ports nimbus
                                                               existing-assignments
                                                               topology->executors
                                                               topology->alive-executors)
        topology->scheduler-assignment (compute-topology->scheduler-assignment nimbus
                                                                               existing-assignments
                                                                               topology->alive-executors)
                                                                               
        missing-assignment-topologies (->> topologies
                                           .getTopologies
                                           (map (memfn getId))
                                           (filter (fn [t]
                                                      (let [alle (get topology->executors t)
                                                            alivee (get topology->alive-executors t)]
                                                            (or (empty? alle)
                                                                (not= alle alivee)
                                                                (< (-> topology->scheduler-assignment
                                                                       (get t)
                                                                       num-used-workers )
                                                                   (-> topologies (.getById t) .getNumWorkers)
                                                                   ))
                                                            ))))
        all-scheduling-slots (->> (all-scheduling-slots nimbus topologies missing-assignment-topologies)
                                  (map (fn [[node-id port]] {node-id #{port}}))
                                  (apply merge-with set/union))
        
        supervisors (read-all-supervisor-details nimbus all-scheduling-slots supervisor->dead-ports)
        cluster (Cluster. (:inimbus nimbus) supervisors topology->scheduler-assignment)

        ;; call scheduler.schedule to schedule all the topologies
        ;; the new assignments for all the topologies are in the cluster object.
        _ (.schedule (:scheduler nimbus) topologies cluster)
        new-scheduler-assignments (.getAssignments cluster)
        ;; add more information to convert SchedulerAssignment to Assignment
        new-topology->executor->node+port (compute-topology->executor->node+port new-scheduler-assignments)]
    ;; print some useful information.
    (doseq [[topology-id executor->node+port] new-topology->executor->node+port
            :let [old-executor->node+port (-> topology-id
                                          existing-assignments
                                          :executor->node+port)
                  reassignment (filter (fn [[executor node+port]]
                                         (and (contains? old-executor->node+port executor)
                                              (not (= node+port (old-executor->node+port executor)))))
                                       executor->node+port)]]
      (when-not (empty? reassignment)
        (let [new-slots-cnt (count (set (vals executor->node+port)))
              reassign-executors (keys reassignment)]
          (log-message "Reassigning " topology-id " to " new-slots-cnt " slots")
          (log-message "Reassign executors: " (vec reassign-executors)))))

    new-topology->executor->node+port))

(defn changed-executors [executor->node+port new-executor->node+port]
  (let [slot-assigned (reverse-map executor->node+port)
        new-slot-assigned (reverse-map new-executor->node+port)
        brand-new-slots (map-diff slot-assigned new-slot-assigned)]
    (apply concat (vals brand-new-slots))
    ))

(defn newly-added-slots [existing-assignment new-assignment]
  (let [old-slots (-> (:executor->node+port existing-assignment)
                      vals
                      set)
        new-slots (-> (:executor->node+port new-assignment)
                      vals
                      set)]
    (set/difference new-slots old-slots)))


(defn basic-supervisor-details-map [storm-cluster-state]
  (let [infos (all-supervisor-info storm-cluster-state)]
    (->> infos
         (map (fn [[id info]]
                 [id (SupervisorDetails. id (:hostname info) (:scheduler-meta info) nil)]))
         (into {}))))

(defn- to-worker-slot [[node port]]
  (WorkerSlot. node port))

;; get existing assignment (just the executor->node+port map) -> default to {}
;; filter out ones which have a executor timeout
;; figure out available slots on cluster. add to that the used valid slots to get total slots. figure out how many executors should be in each slot (e.g., 4, 4, 4, 5)
;; only keep existing slots that satisfy one of those slots. for rest, reassign them across remaining slots
;; edge case for slots with no executor timeout but with supervisor timeout... just treat these as valid slots that can be reassigned to. worst comes to worse the executor will timeout and won't assign here next time around
(defnk mk-assignments [nimbus :scratch-topology-id nil]
  (let [conf (:conf nimbus)
        storm-cluster-state (:storm-cluster-state nimbus)
        ^INimbus inimbus (:inimbus nimbus) 
        ;; read all the topologies
        topology-ids (.active-storms storm-cluster-state)
        topologies (into {} (for [tid topology-ids]
                              {tid (read-topology-details nimbus tid)}))
        topologies (Topologies. topologies)
        ;; read all the assignments
        assigned-topology-ids (.assignments storm-cluster-state nil)
        existing-assignments (into {} (for [tid assigned-topology-ids]
                                        ;; for the topology which wants rebalance (specified by the scratch-topology-id)
                                        ;; we exclude its assignment, meaning that all the slots occupied by its assignment
                                        ;; will be treated as free slot in the scheduler code.
                                        (when (or (nil? scratch-topology-id) (not= tid scratch-topology-id))
                                          {tid (.assignment-info storm-cluster-state tid nil)})))
        ;; make the new assignments for topologies
        topology->executor->node+port (compute-new-topology->executor->node+port
                                       nimbus
                                       existing-assignments
                                       topologies
                                       scratch-topology-id)
        
        
        now-secs (current-time-secs)
        
        basic-supervisor-details-map (basic-supervisor-details-map storm-cluster-state)
        
        ;; construct the final Assignments by adding start-times etc into it
        new-assignments (into {} (for [[topology-id executor->node+port] topology->executor->node+port
                                        :let [existing-assignment (get existing-assignments topology-id)
                                              all-nodes (->> executor->node+port vals (map first) set)
                                              node->host (->> all-nodes
                                                              (mapcat (fn [node]
                                                                        (if-let [host (.getHostName inimbus basic-supervisor-details-map node)]
                                                                          [[node host]]
                                                                          )))
                                                              (into {}))
                                              all-node->host (merge (:node->host existing-assignment) node->host)
                                              reassign-executors (changed-executors (:executor->node+port existing-assignment) executor->node+port)
                                              start-times (merge (:executor->start-time-secs existing-assignment)
                                                                (into {}
                                                                      (for [id reassign-executors]
                                                                        [id now-secs]
                                                                        )))]]
                                   {topology-id (Assignment.
                                                 (master-stormdist-root conf topology-id)
                                                 (select-keys all-node->host all-nodes)
                                                 executor->node+port
                                                 start-times)}))]

    ;; tasks figure out what tasks to talk to by looking at topology at runtime
    ;; only log/set when there's been a change to the assignment
    (doseq [[topology-id assignment] new-assignments
            :let [existing-assignment (get existing-assignments topology-id)
                  topology-details (.getById topologies topology-id)]]
      (if (= existing-assignment assignment)
        (log-debug "Assignment for " topology-id " hasn't changed")
        (do
          (log-message "Setting new assignment for topology id " topology-id ": " (pr-str assignment))
          (.set-assignment! storm-cluster-state topology-id assignment)
          )))
    (->> new-assignments
          (map (fn [[topology-id assignment]]
            (let [existing-assignment (get existing-assignments topology-id)]
              [topology-id (map to-worker-slot (newly-added-slots existing-assignment assignment))] 
              )))
          (into {})
          (.assignSlots inimbus topologies))
    ))

(defn- start-storm [nimbus storm-name storm-id topology-initial-status]
  {:pre [(#{:active :inactive} topology-initial-status)]}                
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        conf (:conf nimbus)
        storm-conf (read-storm-conf conf storm-id)
        topology (system-topology! storm-conf (read-storm-topology conf storm-id))
        num-executors (->> (all-components topology) (map-val num-start-executors))]
    (log-message "Activating " storm-name ": " storm-id)
    (.activate-storm! storm-cluster-state
                      storm-id
                      (StormBase. storm-name
                                  (current-time-secs)
                                  {:type topology-initial-status}
                                  (storm-conf TOPOLOGY-WORKERS)
                                  num-executors))))

;; Master:
;; job submit:
;; 1. read which nodes are available
;; 2. set assignments
;; 3. start storm - necessary in case master goes down, when goes back up can remember to take down the storm (2 states: on or off)

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
        error-ids (set (.error-topologies storm-cluster-state))
        code-ids (code-ids conf)
        assigned-ids (set (.active-storms storm-cluster-state))]
    (set/difference (set/union heartbeat-ids error-ids code-ids) assigned-ids)
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

(defn- component-parallelism [storm-conf component]
  (let [storm-conf (merge storm-conf (component-conf component))
        num-tasks (or (storm-conf TOPOLOGY-TASKS) (num-start-executors component))
        max-parallelism (storm-conf TOPOLOGY-MAX-TASK-PARALLELISM)
        ]
    (if max-parallelism
      (min max-parallelism num-tasks)
      num-tasks)))

(defn normalize-topology [storm-conf ^StormTopology topology]
  (let [ret (.deepCopy topology)]
    (doseq [[_ component] (all-components ret)]
      (.set_json_conf
        (.get_common component)
        (->> {TOPOLOGY-TASKS (component-parallelism storm-conf component)}
             (merge (component-conf component))
             to-json )))
    ret ))

(defn normalize-conf [conf storm-conf ^StormTopology topology]
  ;; ensure that serializations are same for all tasks no matter what's on
  ;; the supervisors. this also allows you to declare the serializations as a sequence
  (let [component-confs (map
                         #(-> (ThriftTopologyUtils/getComponentCommon topology %)
                              .get_json_conf
                              from-json)
                         (ThriftTopologyUtils/getComponentIds topology))
        total-conf (merge conf storm-conf)

        get-merged-conf-val (fn [k merge-fn]
                              (merge-fn
                               (concat
                                (mapcat #(get % k) component-confs)
                                (or (get storm-conf k)
                                    (get conf k)))))]
    ;; topology level serialization registrations take priority
    ;; that way, if there's a conflict, a user can force which serialization to use
    ;; append component conf to storm-conf
    (merge storm-conf
           {TOPOLOGY-KRYO-DECORATORS (get-merged-conf-val TOPOLOGY-KRYO-DECORATORS distinct)
            TOPOLOGY-KRYO-REGISTER (get-merged-conf-val TOPOLOGY-KRYO-REGISTER mapify-serializations)
            TOPOLOGY-ACKER-EXECUTORS (total-conf TOPOLOGY-ACKER-EXECUTORS)
            TOPOLOGY-MAX-TASK-PARALLELISM (total-conf TOPOLOGY-MAX-TASK-PARALLELISM)})))

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
          (.teardown-topology-errors! storm-cluster-state id)
          (rmr (master-stormdist-root conf id))
          (swap! (:heartbeats-cache nimbus) dissoc id))
        ))))

(defn- file-older-than? [now seconds file]
  (<= (+ (.lastModified file) (to-millis seconds)) (to-millis now)))

(defn clean-inbox [dir-location seconds]
  "Deletes jar files in dir older than seconds."
  (let [now (current-time-secs)
        pred #(and (.isFile %) (file-older-than? now seconds %))
        files (filter pred (file-seq (File. dir-location)))]
    (doseq [f files]
      (if (.delete f)
        (log-message "Cleaning inbox ... deleted: " (.getName f))
        ;; This should never happen
        (log-error "Cleaning inbox ... error deleting: " (.getName f))
        ))))

(defn cleanup-corrupt-topologies! [nimbus]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        code-ids (set (code-ids (:conf nimbus)))
        active-topologies (set (.active-storms storm-cluster-state))
        corrupt-topologies (set/difference active-topologies code-ids)]
    (doseq [corrupt corrupt-topologies]
      (log-message "Corrupt topology " corrupt " has state on zookeeper but doesn't have a local dir on Nimbus. Cleaning up...")
      (.remove-storm! storm-cluster-state corrupt)
      )))

(defn- get-errors [storm-cluster-state storm-id component-id]
  (->> (.errors storm-cluster-state storm-id component-id)
       (map #(doto (ErrorInfo. (:error %) (:time-secs %))
                   (.set_host (:host %))
                   (.set_port (:port %))))))

(defn- thriftify-executor-id [[first-task-id last-task-id]]
  (ExecutorInfo. (int first-task-id) (int last-task-id)))

(def DISALLOWED-TOPOLOGY-NAME-STRS #{"/" "." ":" "\\"})

(defn validate-topology-name! [name]
  (if (some #(.contains name %) DISALLOWED-TOPOLOGY-NAME-STRS)
    (throw (InvalidTopologyException.
            (str "Topology name cannot contain any of the following: " (pr-str DISALLOWED-TOPOLOGY-NAME-STRS))))
  (if (clojure.string/blank? name) 
    (throw (InvalidTopologyException. 
            ("Topology name cannot be blank"))))))

(defn- try-read-storm-conf [conf storm-id]
  (try-cause
    (read-storm-conf conf storm-id)
    (catch FileNotFoundException e
       (throw (NotAliveException. storm-id)))
  )
)

(defn- try-read-storm-topology [conf storm-id]
  (try-cause
    (read-storm-topology conf storm-id)
    (catch FileNotFoundException e
       (throw (NotAliveException. storm-id)))
  )
)

(defserverfn service-handler [conf inimbus]
  (.prepare inimbus conf (master-inimbus-dir conf))
  (log-message "Starting Nimbus with conf " conf)
  (let [nimbus (nimbus-data conf inimbus)]
    (.prepare ^backtype.storm.nimbus.ITopologyValidator (:validator nimbus) conf)
    (cleanup-corrupt-topologies! nimbus)
    (doseq [storm-id (.active-storms (:storm-cluster-state nimbus))]
      (transition! nimbus storm-id :startup))
    (schedule-recurring (:timer nimbus)
                        0
                        (conf NIMBUS-MONITOR-FREQ-SECS)
                        (fn []
                          (when (conf NIMBUS-REASSIGN)
                            (locking (:submit-lock nimbus)
                              (mk-assignments nimbus)))
                          (do-cleanup nimbus)
                          ))
    ;; Schedule Nimbus inbox cleaner
    (schedule-recurring (:timer nimbus)
                        0
                        (conf NIMBUS-CLEANUP-INBOX-FREQ-SECS)
                        (fn []
                          (clean-inbox (inbox nimbus) (conf NIMBUS-INBOX-JAR-EXPIRATION-SECS))
                          ))    
    (reify Nimbus$Iface
      (^void submitTopologyWithOpts
        [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology
         ^SubmitOptions submitOptions]
        (try
          (assert (not-nil? submitOptions))
          (validate-topology-name! storm-name)
          (check-storm-active! nimbus storm-name false)
          (let [topo-conf (from-json serializedConf)]
            (try
              (validate-configs-with-schemas topo-conf)
              (catch IllegalArgumentException ex
                (throw (InvalidTopologyException. (.getMessage ex)))))
            (.validate ^backtype.storm.nimbus.ITopologyValidator (:validator nimbus)
                       storm-name
                       topo-conf
                       topology))
          (swap! (:submitted-count nimbus) inc)
          (let [storm-id (str storm-name "-" @(:submitted-count nimbus) "-" (current-time-secs))
                storm-conf (normalize-conf
                            conf
                            (-> serializedConf
                                from-json
                                (assoc STORM-ID storm-id)
                              (assoc TOPOLOGY-NAME storm-name))
                            topology)
                total-storm-conf (merge conf storm-conf)
                topology (normalize-topology total-storm-conf topology)
                storm-cluster-state (:storm-cluster-state nimbus)]
            (system-topology! total-storm-conf topology) ;; this validates the structure of the topology
            (log-message "Received topology submission for " storm-name " with conf " storm-conf)
            ;; lock protects against multiple topologies being submitted at once and
            ;; cleanup thread killing topology in b/w assignment and starting the topology
            (locking (:submit-lock nimbus)
              (setup-storm-code conf storm-id uploadedJarLocation storm-conf topology)
              (.setup-heartbeats! storm-cluster-state storm-id)
              (let [thrift-status->kw-status {TopologyInitialStatus/INACTIVE :inactive
                                              TopologyInitialStatus/ACTIVE :active}]
                (start-storm nimbus storm-name storm-id (thrift-status->kw-status (.get_initial_status submitOptions))))
              (mk-assignments nimbus)))
          (catch Throwable e
            (log-warn-error e "Topology submission exception. (topology name='" storm-name "')")
            (throw e))))
      
      (^void submitTopology
        [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology]
        (.submitTopologyWithOpts this storm-name uploadedJarLocation serializedConf topology
                                 (SubmitOptions. TopologyInitialStatus/ACTIVE)))
      
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
                         (.get_wait_secs options))
              num-workers (if (.is_set_num_workers options)
                            (.get_num_workers options))
              executor-overrides (if (.is_set_num_executors options)
                                   (.get_num_executors options)
                                   {})]
          (doseq [[c num-executors] executor-overrides]
            (when (<= num-executors 0)
              (throw (InvalidTopologyException. "Number of executors must be greater than 0"))
              ))
          (transition-name! nimbus storm-name [:rebalance wait-amt num-workers executor-overrides] true)
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

      (^String getNimbusConf [this]
        (to-json (:conf nimbus)))

      (^String getTopologyConf [this ^String id]
        (to-json (try-read-storm-conf conf id)))

      (^StormTopology getTopology [this ^String id]
        (system-topology! (try-read-storm-conf conf id) (try-read-storm-topology conf id)))

      (^StormTopology getUserTopology [this ^String id]
        (try-read-storm-topology conf id))

      (^ClusterSummary getClusterInfo [this]
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              supervisor-infos (all-supervisor-info storm-cluster-state)
              ;; TODO: need to get the port info about supervisors...
              ;; in standalone just look at metadata, otherwise just say N/A?
              supervisor-summaries (dofor [[id info] supervisor-infos]
                                          (let [ports (set (:meta info)) ;;TODO: this is only true for standalone
                                                ]
                                            (SupervisorSummary. (:hostname info)
                                                                (:uptime-secs info)
                                                                (count ports)
                                                                (count (:used-ports info))
                                                                id )
                                            ))
              nimbus-uptime ((:uptime nimbus))
              bases (topology-bases storm-cluster-state)
              topology-summaries (dofor [[id base] bases]
                                        (let [assignment (.assignment-info storm-cluster-state id nil)]
                                          (TopologySummary. id
                                                            (:storm-name base)
                                                            (->> (:executor->node+port assignment)
                                                                 keys
                                                                 (mapcat executor-id->tasks)
                                                                 count) 
                                                            (->> (:executor->node+port assignment)
                                                                 keys
                                                                 count)                                                            
                                                            (->> (:executor->node+port assignment)
                                                                 vals
                                                                 set
                                                                 count)
                                                            (time-delta (:launch-time-secs base))
                                                            (extract-status-str base))
                                          ))]
          (ClusterSummary. supervisor-summaries
                           nimbus-uptime
                           topology-summaries)
          ))
      
      (^TopologyInfo getTopologyInfo [this ^String storm-id]
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              task->component (storm-task-info (try-read-storm-topology conf storm-id) (try-read-storm-conf conf storm-id))
              base (.storm-base storm-cluster-state storm-id nil)
              assignment (.assignment-info storm-cluster-state storm-id nil)
              beats (.executor-beats storm-cluster-state storm-id (:executor->node+port assignment))
              all-components (-> task->component reverse-map keys)
              errors (->> all-components
                          (map (fn [c] [c (get-errors storm-cluster-state storm-id c)]))
                          (into {}))
              executor-summaries (dofor [[executor [node port]] (:executor->node+port assignment)]
                                        (let [host (-> assignment :node->host (get node))
                                              heartbeat (get beats executor)
                                              stats (:stats heartbeat)
                                              stats (if stats
                                                      (stats/thriftify-executor-stats stats))]
                                          (doto
                                              (ExecutorSummary. (thriftify-executor-id executor)
                                                                (-> executor first task->component)
                                                                host
                                                                port
                                                                (nil-to-zero (:uptime heartbeat)))
                                            (.set_stats stats))
                                          ))
              ]
          (TopologyInfo. storm-id
                         (:storm-name base)
                         (time-delta (:launch-time-secs base))
                         executor-summaries
                         (extract-status-str base)
                         errors
                         )
          ))
      
      Shutdownable
      (shutdown [this]
        (log-message "Shutting down master")
        (cancel-timer (:timer nimbus))
        (.disconnect (:storm-cluster-state nimbus))
        (.cleanup (:downloaders nimbus))
        (.cleanup (:uploaders nimbus))
        (log-message "Shut down master")
        )
      DaemonCommon
      (waiting? [this]
        (timer-waiting? (:timer nimbus))))))

(defn launch-server! [conf nimbus]
  (validate-distributed-mode! conf)
  (let [service-handler (service-handler conf nimbus)
        options (-> (TNonblockingServerSocket. (int (conf NIMBUS-THRIFT-PORT)))
                    (THsHaServer$Args.)
                    (.workerThreads 64)
                    (.protocolFactory (TBinaryProtocol$Factory. false true (conf NIMBUS-THRIFT-MAX-BUFFER-SIZE)))
                    (.processor (Nimbus$Processor. service-handler))
                    )
       server (THsHaServer. (do (set! (. options maxReadBufferBytes)(conf NIMBUS-THRIFT-MAX-BUFFER-SIZE)) options))]
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

(defn -launch [nimbus]
  (launch-server! (read-storm-config) nimbus))

(defn standalone-nimbus []
  (reify INimbus
    (prepare [this conf local-dir]
      )
    (allSlotsAvailableForScheduling [this supervisors topologies topologies-missing-assignments]
      (->> supervisors
           (mapcat (fn [^SupervisorDetails s]
                     (for [p (.getMeta s)]
                       (WorkerSlot. (.getId s) p))))
           set ))
    (assignSlots [this topology slots]
      )
    (getForcedScheduler [this]
      nil )
    (getHostName [this supervisors node-id]
      (if-let [^SupervisorDetails supervisor (get supervisors node-id)]
        (.getHost supervisor)))
    ))

(defn -main []
  (-launch (standalone-nimbus)))
