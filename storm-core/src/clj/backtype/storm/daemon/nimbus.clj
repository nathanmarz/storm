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
  (:import [org.apache.commons.io FileUtils])
  (:import [java.nio ByteBuffer]
           [java.util Collections List HashMap]
           [backtype.storm.generated NimbusSummary])
  (:import [java.io FileNotFoundException File FileOutputStream])
  (:import [java.net InetAddress])
  (:import [java.nio.channels Channels WritableByteChannel])
  (:import [backtype.storm.security.auth ThriftServer ThriftConnectionType ReqContext AuthUtils])
  (:use [backtype.storm.scheduler.DefaultScheduler])
  (:import [backtype.storm.scheduler INimbus SupervisorDetails WorkerSlot TopologyDetails
            Cluster Topologies SchedulerAssignment SchedulerAssignmentImpl DefaultScheduler ExecutorDetails])
  (:import [backtype.storm.nimbus NimbusInfo])
  (:import [backtype.storm.utils TimeCacheMap TimeCacheMap$ExpiredCallback Utils ThriftTopologyUtils
            BufferFileInputStream])
  (:import [backtype.storm.generated NotAliveException AlreadyAliveException StormTopology ErrorInfo
            ExecutorInfo InvalidTopologyException Nimbus$Iface Nimbus$Processor SubmitOptions TopologyInitialStatus
            KillOptions RebalanceOptions ClusterSummary SupervisorSummary TopologySummary TopologyInfo TopologyHistoryInfo
            ExecutorSummary AuthorizationException GetInfoOptions NumErrorsChoice
            ComponentPageInfo TopologyPageInfo LogConfig LogLevel LogLevelAction
            ProfileRequest ProfileAction NodeInfo])
  (:import [backtype.storm.daemon Shutdownable])
  (:use [backtype.storm util config log timer zookeeper local-state])
  (:require [backtype.storm [cluster :as cluster]
                            [converter :as converter]
                            [stats :as stats]
                            [tuple :as tuple]])
  (:require [clojure.set :as set])
  (:import [backtype.storm.daemon.common StormBase Assignment])
  (:use [backtype.storm.daemon common])
  (:import [org.apache.zookeeper data.ACL ZooDefs$Ids ZooDefs$Perms])
  (:import [backtype.storm.utils VersionInfo])
  (:require [clj-time.core :as time])
  (:require [clj-time.coerce :as coerce])
  (:require [metrics.meters :refer [defmeter mark!]])
  (:require [metrics.gauges :refer [defgauge]])
  (:gen-class
    :methods [^{:static true} [launch [backtype.storm.scheduler.INimbus] void]]))

(defmeter nimbus:num-submitTopologyWithOpts-calls)
(defmeter nimbus:num-submitTopology-calls)
(defmeter nimbus:num-killTopologyWithOpts-calls)
(defmeter nimbus:num-killTopology-calls)
(defmeter nimbus:num-rebalance-calls)
(defmeter nimbus:num-activate-calls)
(defmeter nimbus:num-deactivate-calls)
(defmeter nimbus:num-debug-calls)
(defmeter nimbus:num-setWorkerProfiler-calls)
(defmeter nimbus:num-getComponentPendingProfileActions-calls)
(defmeter nimbus:num-setLogConfig-calls)
(defmeter nimbus:num-uploadNewCredentials-calls)
(defmeter nimbus:num-beginFileUpload-calls)
(defmeter nimbus:num-uploadChunk-calls)
(defmeter nimbus:num-finishFileUpload-calls)
(defmeter nimbus:num-beginFileDownload-calls)
(defmeter nimbus:num-downloadChunk-calls)
(defmeter nimbus:num-getNimbusConf-calls)
(defmeter nimbus:num-getLogConfig-calls)
(defmeter nimbus:num-getTopologyConf-calls)
(defmeter nimbus:num-getTopology-calls)
(defmeter nimbus:num-getUserTopology-calls)
(defmeter nimbus:num-getClusterInfo-calls)
(defmeter nimbus:num-getTopologyInfoWithOpts-calls)
(defmeter nimbus:num-getTopologyInfo-calls)
(defmeter nimbus:num-getTopologyPageInfo-calls)
(defmeter nimbus:num-getComponentPageInfo-calls)
(defmeter nimbus:num-shutdown-calls)

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

(defmulti mk-code-distributor cluster-mode)
(defmulti sync-code cluster-mode)

(defnk is-leader [nimbus :throw-exception true]
  (let [leader-elector (:leader-elector nimbus)]
    (if (.isLeader leader-elector) true
      (if throw-exception
        (let [leader-address (.getLeader leader-elector)]
          (throw (RuntimeException. (str "not a leader, current leader is " leader-address))))))))

(def NIMBUS-ZK-ACLS
  [(first ZooDefs$Ids/CREATOR_ALL_ACL) 
   (ACL. (bit-or ZooDefs$Perms/READ ZooDefs$Perms/CREATE) ZooDefs$Ids/ANYONE_ID_UNSAFE)])

(defn create-tology-action-notifier [conf]
  (when-not (clojure.string/blank? (conf NIMBUS-TOPOLOGY-ACTION-NOTIFIER-PLUGIN))
    (let [instance (new-instance (conf NIMBUS-TOPOLOGY-ACTION-NOTIFIER-PLUGIN))]
      (try
        (.prepare instance conf)
        instance
        (catch Exception e
          (log-warn-error e "Ingoring exception, Could not initialize " (conf NIMBUS-TOPOLOGY-ACTION-NOTIFIER-PLUGIN)))))))

(defn nimbus-data [conf inimbus]
  (let [forced-scheduler (.getForcedScheduler inimbus)]
    {:conf conf
     :nimbus-host-port-info (NimbusInfo/fromConf conf)
     :inimbus inimbus
     :authorization-handler (mk-authorization-handler (conf NIMBUS-AUTHORIZER) conf)
     :impersonation-authorization-handler (mk-authorization-handler (conf NIMBUS-IMPERSONATION-AUTHORIZER) conf)
     :submitted-count (atom 0)
     :storm-cluster-state (cluster/mk-storm-cluster-state conf :acls (when
                                                                       (Utils/isZkAuthenticationConfiguredStormServer
                                                                         conf)
                                                                       NIMBUS-ZK-ACLS) :separate-zk-writer? true)
     :submit-lock (Object.)
     :cred-update-lock (Object.)
     :log-update-lock (Object.)
     :heartbeats-cache (atom {})
     :downloaders (file-cache-map conf)
     :uploaders (file-cache-map conf)
     :uptime (uptime-computer)
     :validator (new-instance (conf NIMBUS-TOPOLOGY-VALIDATOR))
     :timer (mk-timer :kill-fn (fn [t]
                                 (log-error t "Error when processing event")
                                 (exit-process! 20 "Error when processing an event")
                                 ))
     :scheduler (mk-scheduler conf inimbus)
     :leader-elector (zk-leader-elector conf)
     :code-distributor (mk-code-distributor conf)
     :id->sched-status (atom {})
     :node-id->resources (atom {}) ;;resources of supervisors
     :id->resources (atom {}) ;;resources of topologies
     :cred-renewers (AuthUtils/GetCredentialRenewers conf)
     :topology-history-lock (Object.)
     :topo-history-state (nimbus-topo-history-state conf)
     :nimbus-autocred-plugins (AuthUtils/getNimbusAutoCredPlugins conf)
     :nimbus-topology-action-notifier (create-tology-action-notifier conf)
     }))

(defn inbox [nimbus]
  (master-inbox (:conf nimbus)))

(defn- read-storm-conf [conf storm-id]
  (let [stormroot (master-stormdist-root conf storm-id)]
    (merge conf
       (clojurify-structure
         (Utils/fromCompressedJsonConf
           (FileUtils/readFileToByteArray
             (File. (master-stormconf-path stormroot))))))))

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
      {
        :status {:type :killed}
        :topology-action-options {:delay-secs delay :action :kill}})
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
      {:status {:type :rebalancing}
       :prev-status status
       :topology-action-options (-> {:delay-secs delay :action :rebalance}
                                  (assoc-non-nil :num-workers num-workers)
                                  (assoc-non-nil :component->executors executor-overrides))
       })))

(defn do-rebalance [nimbus storm-id status storm-base]
  (let [rebalance-options (:topology-action-options storm-base)]
    (.update-storm! (:storm-cluster-state nimbus)
      storm-id
        (-> {:topology-action-options nil}
          (assoc-non-nil :component->executors (:component->executors rebalance-options))
          (assoc-non-nil :num-workers (:num-workers rebalance-options)))))
  (mk-assignments nimbus :scratch-topology-id storm-id))

(defn state-transitions [nimbus storm-id status storm-base]
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
                                         (-> storm-base
                                             :topology-action-options
                                             :delay-secs)
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
                                              (-> storm-base
                                                  :topology-action-options
                                                  :delay-secs)
                                              :do-rebalance)
                                 nil)
                 :kill (kill-transition nimbus storm-id)
                 :do-rebalance (fn []
                                 (do-rebalance nimbus storm-id status storm-base)
                                 (:type (:prev-status storm-base)))
                 }})

(defn transition!
  ([nimbus storm-id event]
     (transition! nimbus storm-id event false))
  ([nimbus storm-id event error-on-no-transition?]
    (is-leader nimbus)
    (locking (:submit-lock nimbus)
       (let [system-events #{:startup}
             [event & event-args] (if (keyword? event) [event] event)
             storm-base (-> nimbus :storm-cluster-state  (.storm-base storm-id nil))
             status (:status storm-base)]
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
                 transition (-> (state-transitions nimbus storm-id status storm-base)
                                (get (:type status))
                                (get-event event))
                 transition (if (or (nil? transition)
                                    (keyword? transition))
                              (fn [] transition)
                              transition)
                 storm-base-updates (apply transition event-args)
                 storm-base-updates (if (keyword? storm-base-updates) ;if it's just a symbol, that just indicates new status.
                                      {:status {:type storm-base-updates}}
                                      storm-base-updates)]

             (when storm-base-updates
               (.update-storm! (:storm-cluster-state nimbus) storm-id storm-base-updates)))))
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

  (let [assignments (.assignments storm-cluster-state nil)]
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
                             (SupervisorDetails. id (:meta info) (:resources-map info)))

        ret (.allSlotsAvailableForScheduling inimbus
                     supervisor-details
                     topologies
                     (set missing-assignment-topologies)
                     )
        ]
    (dofor [^WorkerSlot slot ret]
      [(.getNodeId slot) (.getPort slot)]
      )))

(defn- setup-storm-code [nimbus conf storm-id tmp-jar-location storm-conf topology]
  (let [stormroot (master-stormdist-root conf storm-id)]
   (log-message "nimbus file location:" stormroot)
   (FileUtils/forceMkdir (File. stormroot))
   (FileUtils/cleanDirectory (File. stormroot))
   (setup-jar conf tmp-jar-location stormroot)
   (FileUtils/writeByteArrayToFile (File. (master-stormcode-path stormroot)) (Utils/serialize topology))
   (FileUtils/writeByteArrayToFile (File. (master-stormconf-path stormroot)) (Utils/toCompressedJsonConf storm-conf))
   (if (:code-distributor nimbus) (.upload (:code-distributor nimbus) stormroot storm-id))
   ))

(defn- wait-for-desired-code-replication [nimbus conf storm-id]
  (let [min-replication-count (conf TOPOLOGY-MIN-REPLICATION-COUNT)
        max-replication-wait-time (conf TOPOLOGY-MAX-REPLICATION-WAIT-TIME-SEC)
        total-wait-time (atom 0)
        current-replication-count (atom (if (:code-distributor nimbus) (.getReplicationCount (:code-distributor nimbus) storm-id) 0))]
  (if (:code-distributor nimbus)
    (while (and (> min-replication-count @current-replication-count)
             (or (= -1 max-replication-wait-time)
               (< @total-wait-time max-replication-wait-time)))
        (sleep-secs 1)
        (log-debug "waiting for desired replication to be achieved.
          min-replication-count = " min-replication-count  " max-replication-wait-time = " max-replication-wait-time
          "current-replication-count = " @current-replication-count " total-wait-time " @total-wait-time)
        (swap! total-wait-time inc)
        (reset! current-replication-count  (.getReplicationCount (:code-distributor nimbus) storm-id))))
  (if (< min-replication-count @current-replication-count)
    (log-message "desired replication count "  min-replication-count " achieved,
      current-replication-count" @current-replication-count)
    (log-message "desired replication count of "  min-replication-count " not achieved but we have hit the max wait time "
      max-replication-wait-time " so moving on with replication count = " @current-replication-count)
    )))

(defn- read-storm-topology [conf storm-id]
  (let [stormroot (master-stormdist-root conf storm-id)]
    (Utils/deserialize
      (FileUtils/readFileToByteArray
        (File. (master-stormcode-path stormroot))
        ) StormTopology)))

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
       :executor-reported-time reported-time
       :heartbeat hb}))

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
                       worker->resources (:worker->resources assignment)
                       ;; making a map from node+port to WorkerSlot with allocated resources
                       node+port->slot (into {} (for [[[node port] [mem-on-heap mem-off-heap cpu]] worker->resources]
                                                  {[node port]
                                                   (doto (WorkerSlot. node port)
                                                     (.allocateResource
                                                       mem-on-heap
                                                       mem-off-heap
                                                       cpu))}))
                       executor->slot (into {} (for [[executor [node port]] executor->node+port]
                                                 ;; filter out the dead executors
                                                 (if (contains? alive-executors executor)
                                                   {(ExecutorDetails. (first executor)
                                                                      (second executor))
                                                    (get node+port->slot [node port])}
                                                   {})))]]
             {tid (SchedulerAssignmentImpl. tid executor->slot)})))

(defn- read-all-supervisor-details [nimbus all-scheduling-slots supervisor->dead-ports]
  "return a map: {supervisor-id SupervisorDetails}"
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
                                                    supervisor-details (SupervisorDetails. sid hostname scheduler-meta all-ports (:resources-map supervisor-info))
                                                    ]]
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

(defn convert-assignments-to-worker->resources [new-scheduler-assignments]
  "convert {topology-id -> SchedulerAssignment} to
           {topology-id -> {[node port] [mem-on-heap mem-off-heap cpu]}}
   Make sure this can deal with other non-RAS schedulers
   later we may further support map-for-any-resources"
  (map-val (fn [^SchedulerAssignment assignment]
             (->> assignment
                  .getExecutorToSlot
                  .values
                  (#(into {} (for [^WorkerSlot slot %]
                              {[(.getNodeId slot) (.getPort slot)]
                               [(.getAllocatedMemOnHeap slot) (.getAllocatedMemOffHeap slot) (.getAllocatedCpu slot)]
                               })))))
           new-scheduler-assignments))

(defn compute-new-topology->executor->node+port [new-scheduler-assignments existing-assignments]
  (let [new-topology->executor->node+port (compute-topology->executor->node+port new-scheduler-assignments)]
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

;; public so it can be mocked out
(defn compute-new-scheduler-assignments [nimbus existing-assignments topologies scratch-topology-id]
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
                                                              (-> topologies (.getById t) .getNumWorkers)))))))
        all-scheduling-slots (->> (all-scheduling-slots nimbus topologies missing-assignment-topologies)
                                  (map (fn [[node-id port]] {node-id #{port}}))
                                  (apply merge-with set/union))

        supervisors (read-all-supervisor-details nimbus all-scheduling-slots supervisor->dead-ports)
        cluster (Cluster. (:inimbus nimbus) supervisors topology->scheduler-assignment conf)

        ;; call scheduler.schedule to schedule all the topologies
        ;; the new assignments for all the topologies are in the cluster object.
        _ (.schedule (:scheduler nimbus) topologies cluster)
        _ (.setResourcesMap cluster @(:id->resources nimbus))
        _ (if-not (conf SCHEDULER-DISPLAY-RESOURCE) (.updateAssignedMemoryForTopologyAndSupervisor cluster topologies))
        _ (reset! (:id->sched-status nimbus) (.getStatusMap cluster))
        _ (reset! (:node-id->resources nimbus) (.getSupervisorsResourcesMap cluster))
        _ (reset! (:id->resources nimbus) (.getResourcesMap cluster))]
    (.getAssignments cluster)))

(defn changed-executors [executor->node+port new-executor->node+port]
  (let [executor->node+port (if executor->node+port (sort executor->node+port) nil)
        new-executor->node+port (if new-executor->node+port (sort new-executor->node+port) nil)
        slot-assigned (reverse-map executor->node+port)
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
                 [id (SupervisorDetails. id (:hostname info) (:scheduler-meta info) nil (:resources-map info))]))
         (into {}))))

(defn- to-worker-slot [[node port]]
  (WorkerSlot. node port))

;; get existing assignment (just the executor->node+port map) -> default to {}
;; filter out ones which have a executor timeout
;; figure out available slots on cluster. add to that the used valid slots to get total slots. figure out how many executors should be in each slot (e.g., 4, 4, 4, 5)
;; only keep existing slots that satisfy one of those slots. for rest, reassign them across remaining slots
;; edge case for slots with no executor timeout but with supervisor timeout... just treat these as valid slots that can be reassigned to. worst comes to worse the executor will timeout and won't assign here next time around
(defnk mk-assignments [nimbus :scratch-topology-id nil]
  (if (is-leader nimbus :throw-exception false)
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
        new-scheduler-assignments (compute-new-scheduler-assignments
                                       nimbus
                                       existing-assignments
                                       topologies
                                       scratch-topology-id)

        topology->executor->node+port (compute-new-topology->executor->node+port new-scheduler-assignments existing-assignments)

        topology->executor->node+port (merge (into {} (for [id assigned-topology-ids] {id nil})) topology->executor->node+port)
        new-assigned-worker->resources (convert-assignments-to-worker->resources new-scheduler-assignments)
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
                                                                        )))
                                              worker->resources (get new-assigned-worker->resources topology-id)]]
                                   {topology-id (Assignment.
                                                 (master-stormdist-root conf topology-id)
                                                 (select-keys all-node->host all-nodes)
                                                 executor->node+port
                                                 start-times
                                                 worker->resources)}))]

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
          (.assignSlots inimbus topologies)))
    (log-message "not a leader, skipping assignments")))

(defn notify-topology-action-listener [nimbus storm-id action]
  (let [topology-action-notifier (:nimbus-topology-action-notifier nimbus)]
    (when (not-nil? topology-action-notifier)
      (try (.notify topology-action-notifier storm-id action)
        (catch Exception e
        (log-warn-error e "Ignoring exception from Topology action notifier for storm-Id " storm-id))))))

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
                                  num-executors
                                  (storm-conf TOPOLOGY-SUBMITTER-USER)
                                  nil
                                  nil
                                  {}))
    (notify-topology-action-listener nimbus storm-name "activate")))

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

(defn check-authorization! 
  ([nimbus storm-name storm-conf operation context]
     (let [aclHandler (:authorization-handler nimbus)
           impersonation-authorizer (:impersonation-authorization-handler nimbus)
           ctx (or context (ReqContext/context))
           check-conf (if storm-conf storm-conf (if storm-name {TOPOLOGY-NAME storm-name}))]
       (log-thrift-access (.requestID ctx) (.remoteAddress ctx) (.principal ctx) operation)
       (if (.isImpersonating ctx)
         (do
          (log-warn "principal: " (.realPrincipal ctx) " is trying to impersonate principal: " (.principal ctx))
          (if impersonation-authorizer
           (if-not (.permit impersonation-authorizer ctx operation check-conf)
             (throw (AuthorizationException. (str "principal " (.realPrincipal ctx) " is not authorized to impersonate
                        principal " (.principal ctx) " from host " (.remoteAddress ctx) " Please see SECURITY.MD to learn
                        how to configure impersonation acls."))))
           (log-warn "impersonation attempt but " NIMBUS-IMPERSONATION-AUTHORIZER " has no authorizer configured. potential
                      security risk, please see SECURITY.MD to learn how to configure impersonation authorizer."))))

       (if aclHandler
         (if-not (.permit aclHandler ctx operation check-conf)
           (throw (AuthorizationException. (str operation (if storm-name (str " on topology " storm-name)) " is not authorized")))
           ))))
  ([nimbus storm-name storm-conf operation]
     (check-authorization! nimbus storm-name storm-conf operation (ReqContext/context))))

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
            TOPOLOGY-EVENTLOGGER-EXECUTORS (total-conf TOPOLOGY-EVENTLOGGER-EXECUTORS)
            TOPOLOGY-MAX-TASK-PARALLELISM (total-conf TOPOLOGY-MAX-TASK-PARALLELISM)})))

(defn do-cleanup [nimbus]
  (if (is-leader nimbus :throw-exception false)
    (let [storm-cluster-state (:storm-cluster-state nimbus)
          conf (:conf nimbus)
          submit-lock (:submit-lock nimbus)]
      (let [to-cleanup-ids (locking submit-lock
                             (cleanup-storm-ids conf storm-cluster-state))]
        (when-not (empty? to-cleanup-ids)
          (doseq [id to-cleanup-ids]
            (log-message "Cleaning up " id)
            (if (:code-distributor nimbus) (.cleanup (:code-distributor nimbus) id))
            (.teardown-heartbeats! storm-cluster-state id)
            (.teardown-topology-errors! storm-cluster-state id)
            (rmr (master-stormdist-root conf id))
            (swap! (:heartbeats-cache nimbus) dissoc id))
          )))
    (log-message "not a leader, skipping cleanup")))

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

(defn clean-topology-history
  "Deletes topologies from history older than minutes."
  [mins nimbus]
  (locking (:topology-history-lock nimbus)
    (let [cutoff-age (- (current-time-secs) (* mins 60))
          topo-history-state (:topo-history-state nimbus)
          curr-history (vec (ls-topo-hist topo-history-state))
          new-history (vec (filter (fn [line]
                                     (> (line :timestamp) cutoff-age)) curr-history))]
      (ls-topo-hist! topo-history-state new-history))))

(defn cleanup-corrupt-topologies! [nimbus]
  (if (is-leader nimbus :throw-exception false)
    (let [storm-cluster-state (:storm-cluster-state nimbus)
          code-ids (set (code-ids (:conf nimbus)))
          active-topologies (set (.active-storms storm-cluster-state))
          corrupt-topologies (set/difference active-topologies code-ids)]
      (doseq [corrupt corrupt-topologies]
        (log-message "Corrupt topology " corrupt " has state on zookeeper but doesn't have a local dir on Nimbus. Cleaning up...")
        (.remove-storm! storm-cluster-state corrupt)
        )))
  (log-message "not a leader, skipping cleanup-corrupt-topologies"))

;;setsup code distributor entries for all current topologies for which code is available locally.
(defn setup-code-distributor [nimbus]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        locally-available-storm-ids (set (code-ids (:conf nimbus)))
        active-topologies (set (.active-storms storm-cluster-state))
        locally-available-active-storm-ids (set/intersection locally-available-storm-ids active-topologies)]
    (doseq [storm-id locally-available-active-storm-ids]
      (.setup-code-distributor! storm-cluster-state storm-id (:nimbus-host-port-info nimbus)))))

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

;; We will only file at <Storm dist root>/<Topology ID>/<File>
;; to be accessed via Thrift
;; ex., storm-local/nimbus/stormdist/aa-1-1377104853/stormjar.jar
(defn check-file-access [conf file-path]
  (log-debug "check file access:" file-path)
  (try
    (if (not= (.getCanonicalFile (File. (master-stormdist-root conf)))
          (-> (File. file-path) .getCanonicalFile .getParentFile .getParentFile))
      (throw (AuthorizationException. (str "Invalid file path: " file-path))))
    (catch Exception e
      (throw (AuthorizationException. (str "Invalid file path: " file-path))))))

(defn try-read-storm-conf [conf storm-id]
  (try-cause
    (read-storm-conf conf storm-id)
    (catch FileNotFoundException e
       (throw (NotAliveException. (str storm-id))))
  )
)

(defn try-read-storm-conf-from-name [conf storm-name nimbus]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        id (get-storm-id storm-cluster-state storm-name)]
   (try-read-storm-conf conf id)))

(defn try-read-storm-topology [conf storm-id]
  (try-cause
    (read-storm-topology conf storm-id)
    (catch FileNotFoundException e
       (throw (NotAliveException. (str storm-id))))
  )
)

(defn add-topology-to-history-log
  [storm-id nimbus topology-conf]
  (log-message "Adding topo to history log: " storm-id)
  (locking (:topology-history-lock nimbus)
    (let [topo-history-state (:topo-history-state nimbus)
          users (get-topo-logs-users topology-conf)
          groups (get-topo-logs-groups topology-conf)
          curr-history (vec (ls-topo-hist topo-history-state))
          new-history (conj curr-history {:topoid storm-id :timestamp (current-time-secs)
                                          :users users :groups groups})]
      (ls-topo-hist! topo-history-state new-history))))

(defn igroup-mapper
  [storm-conf]
  (AuthUtils/GetGroupMappingServiceProviderPlugin storm-conf))

(defn user-groups
  [user storm-conf]
  (if (clojure.string/blank? user) [] (.getGroups (igroup-mapper storm-conf) user)))

(defn does-users-group-intersect?
  "Check to see if any of the users groups intersect with the list of groups passed in"
  [user groups-to-check storm-conf]
  (let [groups (user-groups user storm-conf)]
    (> (.size (set/intersection (set groups) (set groups-to-check))) 0)))

(defn read-topology-history
  [nimbus user admin-users]
  (let [topo-history-state (:topo-history-state nimbus)
        curr-history (vec (ls-topo-hist topo-history-state))
        topo-user-can-access (fn [line user storm-conf]
                               (if (nil? user)
                                 (line :topoid)
                                 (if (or (some #(= % user) admin-users)
                                       (does-users-group-intersect? user (line :groups) storm-conf)
                                       (some #(= % user) (line :users)))
                                   (line :topoid)
                                   nil)))]
    (remove nil? (map #(topo-user-can-access % user (:conf nimbus)) curr-history))))

(defn renew-credentials [nimbus]
  (if (is-leader nimbus :throw-exception false)
    (let [storm-cluster-state (:storm-cluster-state nimbus)
          renewers (:cred-renewers nimbus)
          update-lock (:cred-update-lock nimbus)
          assigned-ids (set (.active-storms storm-cluster-state))]
      (when-not (empty? assigned-ids)
        (doseq [id assigned-ids]
          (locking update-lock
            (let [orig-creds (.credentials storm-cluster-state id nil)
                  topology-conf (try-read-storm-conf (:conf nimbus) id)]
              (if orig-creds
                (let [new-creds (HashMap. orig-creds)]
                  (doseq [renewer renewers]
                    (log-message "Renewing Creds For " id " with " renewer)
                    (.renew renewer new-creds (Collections/unmodifiableMap topology-conf)))
                  (when-not (= orig-creds new-creds)
                    (.set-credentials! storm-cluster-state id new-creds topology-conf)
                    ))))))))
    (log-message "not a leader skipping , credential renweal.")))

(defn validate-topology-size [topo-conf nimbus-conf topology]
  (let [workers-count (get topo-conf TOPOLOGY-WORKERS)
        workers-allowed (get nimbus-conf NIMBUS-SLOTS-PER-TOPOLOGY)
        num-executors (->> (all-components topology) (map-val num-start-executors))
        executors-count (reduce + (vals num-executors))
        executors-allowed (get nimbus-conf NIMBUS-EXECUTORS-PER-TOPOLOGY)]
    (when (and 
           (not (nil? executors-allowed))
           (> executors-count executors-allowed))
      (throw 
       (InvalidTopologyException. 
        (str "Failed to submit topology. Topology requests more than " executors-allowed " executors."))))
    (when (and
           (not (nil? workers-allowed))
           (> workers-count workers-allowed))
      (throw 
       (InvalidTopologyException. 
        (str "Failed to submit topology. Topology requests more than " workers-allowed " workers."))))))

(defn- set-logger-timeouts [log-config]
  (let [timeout-secs (.get_reset_log_level_timeout_secs log-config)
       timeout (time/plus (time/now) (time/secs timeout-secs))]
   (if (time/after? timeout (time/now))
     (.set_reset_log_level_timeout_epoch log-config (coerce/to-long timeout))
     (.unset_reset_log_level_timeout_epoch log-config))))

(defserverfn service-handler [conf inimbus]
  (.prepare inimbus conf (master-inimbus-dir conf))
  (log-message "Starting Nimbus with conf " conf)
  (let [nimbus (nimbus-data conf inimbus)
        principal-to-local (AuthUtils/GetPrincipalToLocalPlugin conf)
        admin-users (or (.get conf NIMBUS-ADMINS) [])
        get-common-topo-info
          (fn [^String storm-id operation]
            (let [storm-cluster-state (:storm-cluster-state nimbus)
                  topology-conf (try-read-storm-conf conf storm-id)
                  storm-name (topology-conf TOPOLOGY-NAME)
                  _ (check-authorization! nimbus
                                          storm-name
                                          topology-conf
                                          operation)
                  topology (try-read-storm-topology conf storm-id)
                  task->component (storm-task-info topology topology-conf)
                  base (.storm-base storm-cluster-state storm-id nil)
                  launch-time-secs (if base (:launch-time-secs base)
                                     (throw
                                       (NotAliveException. (str storm-id))))
                  assignment (.assignment-info storm-cluster-state storm-id nil)
                  beats (map-val :heartbeat (get @(:heartbeats-cache nimbus)
                                                 storm-id))
                  all-components (set (vals task->component))]
              {:storm-name storm-name
               :storm-cluster-state storm-cluster-state
               :all-components all-components
               :launch-time-secs launch-time-secs
               :assignment assignment
               :beats beats
               :topology topology
               :task->component task->component
               :base base}))
        get-last-error (fn [storm-cluster-state storm-id component-id]
                         (if-let [e (.last-error storm-cluster-state
                                                 storm-id
                                                 component-id)]
                           (doto (ErrorInfo. (:error e) (:time-secs e))
                             (.set_host (:host e))
                             (.set_port (:port e)))))]
    (.prepare ^backtype.storm.nimbus.ITopologyValidator (:validator nimbus) conf)

    ;add to nimbuses
    (.add-nimbus-host! (:storm-cluster-state nimbus) (.toHostPortString (:nimbus-host-port-info nimbus))
      (NimbusSummary.
        (.getHost (:nimbus-host-port-info nimbus))
        (.getPort (:nimbus-host-port-info nimbus))
        (current-time-secs)
        false ;is-leader
        (str (VersionInfo/getVersion))))

    (.addToLeaderLockQueue (:leader-elector nimbus))
    (cleanup-corrupt-topologies! nimbus)
    (setup-code-distributor nimbus)

    ;register call back for code-distributor
    (.code-distributor (:storm-cluster-state nimbus) (fn [] (sync-code conf nimbus)))
    (when (is-leader nimbus :throw-exception false)
      (doseq [storm-id (.active-storms (:storm-cluster-state nimbus))]
        (transition! nimbus storm-id :startup)))
    (schedule-recurring (:timer nimbus)
                        0
                        (conf NIMBUS-MONITOR-FREQ-SECS)
                        (fn []
                          (when-not (conf NIMBUS-DO-NOT-REASSIGN)
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
    ;; Schedule topology history cleaner
    (when-let [interval (conf LOGVIEWER-CLEANUP-INTERVAL-SECS)]
      (schedule-recurring (:timer nimbus)
        0
        (conf LOGVIEWER-CLEANUP-INTERVAL-SECS)
        (fn []
          (clean-topology-history (conf LOGVIEWER-CLEANUP-AGE-MINS) nimbus)
          )))
    ;;schedule nimbus code sync thread to sync code from other nimbuses.
    (schedule-recurring (:timer nimbus)
      0
      (conf NIMBUS-CODE-SYNC-FREQ-SECS)
      (fn []
        (sync-code conf nimbus)
        ))

    (schedule-recurring (:timer nimbus)
                        0
                        (conf NIMBUS-CREDENTIAL-RENEW-FREQ-SECS)
                        (fn []
                          (renew-credentials nimbus)))

    (defgauge nimbus:num-supervisors
      (fn [] (.size (.supervisors (:storm-cluster-state nimbus) nil))))

    (start-metrics-reporters)

    (reify Nimbus$Iface
      (^void submitTopologyWithOpts
        [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology
         ^SubmitOptions submitOptions]
        (try
          (mark! nimbus:num-submitTopologyWithOpts-calls)
          (is-leader nimbus)
          (assert (not-nil? submitOptions))
          (validate-topology-name! storm-name)
          (check-authorization! nimbus storm-name nil "submitTopology")
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
                credentials (.get_creds submitOptions)
                credentials (when credentials (.get_creds credentials))
                topo-conf (from-json serializedConf)
                storm-conf-submitted (normalize-conf
                            conf
                            (-> topo-conf
                              (assoc STORM-ID storm-id)
                              (assoc TOPOLOGY-NAME storm-name))
                            topology)
                req (ReqContext/context)
                principal (.principal req)
                submitter-principal (if principal (.toString principal))
                submitter-user (.toLocal principal-to-local principal)
                topo-acl (distinct (remove nil? (conj (.get storm-conf-submitted TOPOLOGY-USERS) submitter-principal, submitter-user)))
                storm-conf (-> storm-conf-submitted
                               (assoc TOPOLOGY-SUBMITTER-PRINCIPAL (if submitter-principal submitter-principal ""))
                               (assoc TOPOLOGY-SUBMITTER-USER (if submitter-user submitter-user "")) ;Don't let the user set who we launch as
                               (assoc TOPOLOGY-USERS topo-acl)
                               (assoc STORM-ZOOKEEPER-SUPERACL (.get conf STORM-ZOOKEEPER-SUPERACL)))
                storm-conf (if (Utils/isZkAuthenticationConfiguredStormServer conf)
                                storm-conf
                                (dissoc storm-conf STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD))
                total-storm-conf (merge conf storm-conf)
                topology (normalize-topology total-storm-conf topology)
                storm-cluster-state (:storm-cluster-state nimbus)]
            (when credentials (doseq [nimbus-autocred-plugin (:nimbus-autocred-plugins nimbus)]
              (.populateCredentials nimbus-autocred-plugin credentials (Collections/unmodifiableMap storm-conf))))
            (if (and (conf SUPERVISOR-RUN-WORKER-AS-USER) (or (nil? submitter-user) (.isEmpty (.trim submitter-user)))) 
              (throw (AuthorizationException. "Could not determine the user to run this topology as.")))
            (system-topology! total-storm-conf topology) ;; this validates the structure of the topology
            (validate-topology-size topo-conf conf topology)
            (when (and (Utils/isZkAuthenticationConfiguredStormServer conf)
                       (not (Utils/isZkAuthenticationConfiguredTopology storm-conf)))
                (throw (IllegalArgumentException. "The cluster is configured for zookeeper authentication, but no payload was provided.")))
            (log-message "Received topology submission for "
                         storm-name
                         " with conf "
                         (redact-value storm-conf STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD))
            ;; lock protects against multiple topologies being submitted at once and
            ;; cleanup thread killing topology in b/w assignment and starting the topology
            (locking (:submit-lock nimbus)
              (check-storm-active! nimbus storm-name false)
              ;;cred-update-lock is not needed here because creds are being added for the first time.
              (.set-credentials! storm-cluster-state storm-id credentials storm-conf)
              (setup-storm-code nimbus conf storm-id uploadedJarLocation storm-conf topology)
              (.setup-code-distributor! storm-cluster-state storm-id (:nimbus-host-port-info nimbus))
              (wait-for-desired-code-replication nimbus total-storm-conf storm-id)
              (.setup-heartbeats! storm-cluster-state storm-id)
              (.setup-backpressure! storm-cluster-state storm-id)
              (notify-topology-action-listener nimbus storm-name "submitTopology")
              (let [thrift-status->kw-status {TopologyInitialStatus/INACTIVE :inactive
                                              TopologyInitialStatus/ACTIVE :active}]
                (start-storm nimbus storm-name storm-id (thrift-status->kw-status (.get_initial_status submitOptions))))))
          (catch Throwable e
            (log-warn-error e "Topology submission exception. (topology name='" storm-name "')")
            (throw e))))
      
      (^void submitTopology
        [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology]
        (mark! nimbus:num-submitTopology-calls)
        (.submitTopologyWithOpts this storm-name uploadedJarLocation serializedConf topology
                                 (SubmitOptions. TopologyInitialStatus/ACTIVE)))
      
      (^void killTopology [this ^String name]
        (mark! nimbus:num-killTopology-calls)
        (.killTopologyWithOpts this name (KillOptions.)))

      (^void killTopologyWithOpts [this ^String storm-name ^KillOptions options]
        (mark! nimbus:num-killTopologyWithOpts-calls)
        (check-storm-active! nimbus storm-name true)
        (let [topology-conf (try-read-storm-conf-from-name conf storm-name nimbus)
              operation "killTopology"]
          (check-authorization! nimbus storm-name topology-conf operation)
          (let [wait-amt (if (.is_set_wait_secs options)
                           (.get_wait_secs options)
                           )]
            (transition-name! nimbus storm-name [:kill wait-amt] true)
            (notify-topology-action-listener nimbus storm-name operation))))

      (^void rebalance [this ^String storm-name ^RebalanceOptions options]
        (mark! nimbus:num-rebalance-calls)
        (check-storm-active! nimbus storm-name true)
        (let [topology-conf (try-read-storm-conf-from-name conf storm-name nimbus)
              operation "rebalance"]
          (check-authorization! nimbus storm-name topology-conf operation)
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

            (notify-topology-action-listener nimbus storm-name operation))))

      (activate [this storm-name]
        (mark! nimbus:num-activate-calls)
        (let [topology-conf (try-read-storm-conf-from-name conf storm-name nimbus)
              operation "activate"]
          (check-authorization! nimbus storm-name topology-conf operation)
          (transition-name! nimbus storm-name :activate true)
          (notify-topology-action-listener nimbus storm-name operation)))

      (deactivate [this storm-name]
        (mark! nimbus:num-deactivate-calls)
        (let [topology-conf (try-read-storm-conf-from-name conf storm-name nimbus)
              operation "deactivate"]
          (check-authorization! nimbus storm-name topology-conf operation)
          (transition-name! nimbus storm-name :inactivate true)
          (notify-topology-action-listener nimbus storm-name operation)))

      (debug [this storm-name component-id enable? samplingPct]
        (mark! nimbus:num-debug-calls)
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              storm-id (get-storm-id storm-cluster-state storm-name)
              topology-conf (try-read-storm-conf conf storm-id)
              ;; make sure samplingPct is within bounds.
              spct (Math/max (Math/min samplingPct 100.0) 0.0)
              ;; while disabling we retain the sampling pct.
              debug-options (if enable? {:enable enable? :samplingpct spct} {:enable enable?})
              storm-base-updates (assoc {} :component->debug (if (empty? component-id)
                                                               {storm-id debug-options}
                                                               {component-id debug-options}))]
          (check-authorization! nimbus storm-name topology-conf "debug")
          (when-not storm-id
            (throw (NotAliveException. storm-name)))
          (log-message "Nimbus setting debug to " enable? " for storm-name '" storm-name "' storm-id '" storm-id "' sampling pct '" spct "'"
            (if (not (clojure.string/blank? component-id)) (str " component-id '" component-id "'")))
          (locking (:submit-lock nimbus)
            (.update-storm! storm-cluster-state storm-id storm-base-updates))))

      (^void setWorkerProfiler
        [this ^String id ^ProfileRequest profileRequest]
        (mark! nimbus:num-setWorkerProfiler-calls)
        (let [topology-conf (try-read-storm-conf conf id)
              storm-name (topology-conf TOPOLOGY-NAME)
              _ (check-authorization! nimbus storm-name topology-conf "setWorkerProfiler")
              storm-cluster-state (:storm-cluster-state nimbus)]
          (.set-worker-profile-request storm-cluster-state id profileRequest)))

      (^List getComponentPendingProfileActions
        [this ^String id ^String component_id ^ProfileAction action]
        (mark! nimbus:num-getComponentPendingProfileActions-calls)
        (let [info (get-common-topo-info id "getComponentPendingProfileActions")
              storm-cluster-state (:storm-cluster-state info)
              task->component (:task->component info)
              {:keys [executor->node+port node->host]} (:assignment info)
              executor->host+port (map-val (fn [[node port]]
                                             [(node->host node) port])
                                    executor->node+port)
              nodeinfos (stats/extract-nodeinfos-from-hb-for-comp executor->host+port task->component false component_id)
              all-pending-actions-for-topology (.get-topology-profile-requests storm-cluster-state id true)
              latest-profile-actions (remove nil? (map (fn [nodeInfo]
                                                         (->> all-pending-actions-for-topology
                                                              (filter #(and (= (:host nodeInfo) (.get_node (.get_nodeInfo %)))
                                                                         (= (:port nodeInfo) (first (.get_port (.get_nodeInfo  %))))))
                                                              (filter #(= action (.get_action %)))
                                                              (sort-by #(.get_time_stamp %) >)
                                                              first))
                                                    nodeinfos))]
          (log-message "Latest profile actions for topology " id " component " component_id " " (pr-str latest-profile-actions))
          latest-profile-actions))

      (^void setLogConfig [this ^String id ^LogConfig log-config-msg]
        (mark! nimbus:num-setLogConfig-calls)
        (let [topology-conf (try-read-storm-conf conf id)
              storm-name (topology-conf TOPOLOGY-NAME)
              _ (check-authorization! nimbus storm-name topology-conf "setLogConfig")
              storm-cluster-state (:storm-cluster-state nimbus)
              merged-log-config (or (.topology-log-config storm-cluster-state id nil) (LogConfig.))
              named-loggers (.get_named_logger_level merged-log-config)]
            (doseq [[_ level] named-loggers]
              (.set_action level LogLevelAction/UNCHANGED))
            (doseq [[logger-name log-config] (.get_named_logger_level log-config-msg)]
              (let [action (.get_action log-config)]
                (if (clojure.string/blank? logger-name)
                  (throw (RuntimeException. "Named loggers need a valid name. Use ROOT for the root logger")))
                (condp = action
                  LogLevelAction/UPDATE
                    (do (set-logger-timeouts log-config)
                          (.put_to_named_logger_level merged-log-config logger-name log-config))
                  LogLevelAction/REMOVE
                    (let [named-loggers (.get_named_logger_level merged-log-config)]
                      (if (and (not (nil? named-loggers))
                               (.containsKey named-loggers logger-name))
                        (.remove named-loggers logger-name))))))
            (log-message "Setting log config for " storm-name ":" merged-log-config)
            (.set-topology-log-config! storm-cluster-state id merged-log-config)))

      (uploadNewCredentials [this storm-name credentials]
        (mark! nimbus:num-uploadNewCredentials-calls)
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              storm-id (get-storm-id storm-cluster-state storm-name)
              topology-conf (try-read-storm-conf conf storm-id)
              creds (when credentials (.get_creds credentials))]
          (check-authorization! nimbus storm-name topology-conf "uploadNewCredentials")
          (locking (:cred-update-lock nimbus) (.set-credentials! storm-cluster-state storm-id creds topology-conf))))

      (beginFileUpload [this]
        (mark! nimbus:num-beginFileUpload-calls)
        (check-authorization! nimbus nil nil "fileUpload")
        (let [fileloc (str (inbox nimbus) "/stormjar-" (uuid) ".jar")]
          (.put (:uploaders nimbus)
                fileloc
                (Channels/newChannel (FileOutputStream. fileloc)))
          (log-message "Uploading file from client to " fileloc)
          fileloc
          ))

      (^void uploadChunk [this ^String location ^ByteBuffer chunk]
        (mark! nimbus:num-uploadChunk-calls)
        (check-authorization! nimbus nil nil "fileUpload")
        (let [uploaders (:uploaders nimbus)
              ^WritableByteChannel channel (.get uploaders location)]
          (when-not channel
            (throw (RuntimeException.
                    "File for that location does not exist (or timed out)")))
          (.write channel chunk)
          (.put uploaders location channel)
          ))

      (^void finishFileUpload [this ^String location]
        (mark! nimbus:num-finishFileUpload-calls)
        (check-authorization! nimbus nil nil "fileUpload")
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
        (mark! nimbus:num-beginFileDownload-calls)
        (check-authorization! nimbus nil nil "fileDownload")
        (check-file-access (:conf nimbus) file)
        (let [is (BufferFileInputStream. file)
              id (uuid)]
          (.put (:downloaders nimbus) id is)
          id
          ))

      (^ByteBuffer downloadChunk [this ^String id]
        (mark! nimbus:num-downloadChunk-calls)
        (check-authorization! nimbus nil nil "fileDownload")
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
        (mark! nimbus:num-getNimbusConf-calls)
        (check-authorization! nimbus nil nil "getNimbusConf")
        (to-json (:conf nimbus)))

      (^LogConfig getLogConfig [this ^String id]
        (mark! nimbus:num-getLogConfig-calls)
        (let [topology-conf (try-read-storm-conf conf id)
              storm-name (topology-conf TOPOLOGY-NAME)
              _ (check-authorization! nimbus storm-name topology-conf "getLogConfig")
             storm-cluster-state (:storm-cluster-state nimbus)
             log-config (.topology-log-config storm-cluster-state id nil)]
           (if log-config log-config (LogConfig.))))

      (^String getTopologyConf [this ^String id]
        (mark! nimbus:num-getTopologyConf-calls)
        (let [topology-conf (try-read-storm-conf conf id)
              storm-name (topology-conf TOPOLOGY-NAME)]
              (check-authorization! nimbus storm-name topology-conf "getTopologyConf")
              (to-json topology-conf)))

      (^StormTopology getTopology [this ^String id]
        (mark! nimbus:num-getTopology-calls)
        (let [topology-conf (try-read-storm-conf conf id)
              storm-name (topology-conf TOPOLOGY-NAME)]
              (check-authorization! nimbus storm-name topology-conf "getTopology")
              (system-topology! topology-conf (try-read-storm-topology conf id))))

      (^StormTopology getUserTopology [this ^String id]
        (mark! nimbus:num-getUserTopology-calls)
        (let [topology-conf (try-read-storm-conf conf id)
              storm-name (topology-conf TOPOLOGY-NAME)]
              (check-authorization! nimbus storm-name topology-conf "getUserTopology")
              (try-read-storm-topology topology-conf id)))

      (^ClusterSummary getClusterInfo [this]
        (mark! nimbus:num-getClusterInfo-calls)
        (check-authorization! nimbus nil nil "getClusterInfo")
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              supervisor-infos (all-supervisor-info storm-cluster-state)
              ;; TODO: need to get the port info about supervisors...
              ;; in standalone just look at metadata, otherwise just say N/A?
              supervisor-summaries (dofor [[id info] supervisor-infos]
                                     (let [ports (set (:meta info)) ;;TODO: this is only true for standalone
                                           sup-sum (SupervisorSummary. (:hostname info)
                                                     (:uptime-secs info)
                                                     (count ports)
                                                     (count (:used-ports info))
                                                     id) ]
                                       (.set_total_resources sup-sum (map-val double (:resources-map info)))
                                       (when-let [[total-mem total-cpu used-mem used-cpu] (.get @(:node-id->resources nimbus) id)]
                                         (.set_used_mem sup-sum used-mem)
                                         (.set_used_cpu sup-sum used-cpu))
                                       (when-let [version (:version info)] (.set_version sup-sum version))
                                       sup-sum))
              nimbus-uptime ((:uptime nimbus))
              bases (topology-bases storm-cluster-state)
              nimbuses (.nimbuses storm-cluster-state)

              ;;update the isLeader field for each nimbus summary
              _ (let [leader (.getLeader (:leader-elector nimbus))
                      leader-host (.getHost leader)
                      leader-port (.getPort leader)]
                  (doseq [nimbus-summary nimbuses]
                    (.set_uptime_secs nimbus-summary (time-delta (.get_uptime_secs nimbus-summary)))
                    (.set_isLeader nimbus-summary (and (= leader-host (.get_host nimbus-summary)) (= leader-port (.get_port nimbus-summary))))))

              topology-summaries (dofor [[id base] bases :when base]
	                                  (let [assignment (.assignment-info storm-cluster-state id nil)
                                                topo-summ (TopologySummary. id
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
                                                            (extract-status-str base))]
                                               (when-let [owner (:owner base)] (.set_owner topo-summ owner))
                                               (when-let [sched-status (.get @(:id->sched-status nimbus) id)] (.set_sched_status topo-summ sched-status))
                                               (when-let [resources (.get @(:id->resources nimbus) id)]
                                                 (.set_requested_memonheap topo-summ (get resources 0))
                                                 (.set_requested_memoffheap topo-summ (get resources 1))
                                                 (.set_requested_cpu topo-summ (get resources 2))
                                                 (.set_assigned_memonheap topo-summ (get resources 3))
                                                 (.set_assigned_memoffheap topo-summ (get resources 4))
                                                 (.set_assigned_cpu topo-summ (get resources 5)))
                                               (.set_replication_count topo-summ (if (:code-distributor nimbus)
                                                                                   (.getReplicationCount (:code-distributor nimbus) id)
                                                                                   1))
                                               topo-summ
                                          ))
              ret (ClusterSummary. supervisor-summaries
                                   topology-summaries
                                   nimbuses)
              _ (.set_nimbus_uptime_secs ret nimbus-uptime)]
              ret))
      
      (^TopologyInfo getTopologyInfoWithOpts [this ^String storm-id ^GetInfoOptions options]
        (mark! nimbus:num-getTopologyInfoWithOpts-calls)
        (let [{:keys [storm-name
                      storm-cluster-state
                      all-components
                      launch-time-secs
                      assignment
                      beats
                      task->component
                      base]} (get-common-topo-info storm-id "getTopologyInfo")
              num-err-choice (or (.get_num_err_choice options)
                                 NumErrorsChoice/ALL)
              errors-fn (condp = num-err-choice
                          NumErrorsChoice/NONE (fn [& _] ()) ;; empty list only
                          NumErrorsChoice/ONE (comp #(remove nil? %)
                                                    list
                                                    get-last-error)
                          NumErrorsChoice/ALL get-errors
                          ;; Default
                          (do
                            (log-warn "Got invalid NumErrorsChoice '"
                                      num-err-choice
                                      "'")
                            get-errors))
              errors (->> all-components
                          (map (fn [c] [c (errors-fn storm-cluster-state storm-id c)]))
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
              topo-info  (TopologyInfo. storm-id
                           storm-name
                           (time-delta launch-time-secs)
                           executor-summaries
                           (extract-status-str base)
                           errors
                           )]
            (when-let [owner (:owner base)] (.set_owner topo-info owner))
            (when-let [sched-status (.get @(:id->sched-status nimbus) storm-id)] (.set_sched_status topo-info sched-status))
            (when-let [resources (.get @(:id->resources nimbus) storm-id)]
              (.set_requested_memonheap topo-info (get resources 0))
              (.set_requested_memoffheap topo-info (get resources 1))
              (.set_requested_cpu topo-info (get resources 2))
              (.set_assigned_memonheap topo-info (get resources 3))
              (.set_assigned_memoffheap topo-info (get resources 4))
              (.set_assigned_cpu topo-info (get resources 5)))
            (when-let [component->debug (:component->debug base)]
              (.set_component_debug topo-info (map-val converter/thriftify-debugoptions component->debug)))
            (.set_replication_count topo-info (if (:code-distributor nimbus) (.getReplicationCount (:code-distributor nimbus) storm-id) 1))

            topo-info
          ))

      (^TopologyInfo getTopologyInfo [this ^String topology-id]
        (mark! nimbus:num-getTopologyInfo-calls)
        (.getTopologyInfoWithOpts this
                                  topology-id
                                  (doto (GetInfoOptions.) (.set_num_err_choice NumErrorsChoice/ALL))))

      (^TopologyPageInfo getTopologyPageInfo
        [this ^String topo-id ^String window ^boolean include-sys?]
        (mark! nimbus:num-getTopologyPageInfo-calls)
        (let [info (get-common-topo-info topo-id "getTopologyPageInfo")

              exec->node+port (:executor->node+port (:assignment info))
              last-err-fn (partial get-last-error
                                   (:storm-cluster-state info)
                                   topo-id)
              topo-page-info (stats/agg-topo-execs-stats topo-id
                                                         exec->node+port
                                                         (:task->component info)
                                                         (:beats info)
                                                         (:topology info)
                                                         window
                                                         include-sys?
                                                         last-err-fn)]
          (when-let [owner (:owner (:base info))]
            (.set_owner topo-page-info owner))
          (when-let [sched-status (.get @(:id->sched-status nimbus) topo-id)]
            (.set_sched_status topo-page-info sched-status))
          (when-let [resources (.get @(:id->resources nimbus) topo-id)]
            (.set_requested_memonheap topo-page-info (get resources 0))
            (.set_requested_memoffheap topo-page-info (get resources 1))
            (.set_requested_cpu topo-page-info (get resources 2))
            (.set_assigned_memonheap topo-page-info (get resources 3))
            (.set_assigned_memoffheap topo-page-info (get resources 4))
            (.set_assigned_cpu topo-page-info (get resources 5)))
          (doto topo-page-info
            (.set_name (:storm-name info))
            (.set_status (extract-status-str (:base info)))
            (.set_uptime_secs (time-delta (:launch-time-secs info)))
            (.set_topology_conf (to-json (try-read-storm-conf conf
                                                              topo-id)))
            (.set_replication_count
              (.getReplicationCount (:code-distributor nimbus) topo-id)))
          (when-let [debug-options
                     (get-in info [:base :component->debug topo-id])]
            (.set_debug_options
              topo-page-info
              (converter/thriftify-debugoptions debug-options)))
          topo-page-info))

      (^ComponentPageInfo getComponentPageInfo
        [this
         ^String topo-id
         ^String component-id
         ^String window
         ^boolean include-sys?]
        (mark! nimbus:num-getComponentPageInfo-calls)
        (let [info (get-common-topo-info topo-id "getComponentPageInfo")
              {:keys [executor->node+port node->host]} (:assignment info)
              executor->host+port (map-val (fn [[node port]]
                                             [(node->host node) port])
                                           executor->node+port)
              comp-page-info (stats/agg-comp-execs-stats executor->host+port
                                                         (:task->component info)
                                                         (:beats info)
                                                         window
                                                         include-sys?
                                                         topo-id
                                                         (:topology info)
                                                         component-id)]
          (doto comp-page-info
            (.set_topology_name (:storm-name info))
            (.set_errors (get-errors (:storm-cluster-state info)
                                     topo-id
                                     component-id))
            (.set_topology_status (extract-status-str (:base info))))
          (when-let [debug-options
                     (get-in info [:base :component->debug component-id])]
            (.set_debug_options
              comp-page-info
              (converter/thriftify-debugoptions debug-options)))
          ;; Add the event logger details.
          (let [component->tasks (reverse-map (:task->component info))
                eventlogger-tasks (sort (get component->tasks
                                             EVENTLOGGER-COMPONENT-ID))
                ;; Find the task the events from this component route to.
                task-index (mod (tuple/list-hash-code [component-id])
                                (count eventlogger-tasks))
                task-id (nth eventlogger-tasks task-index)
                eventlogger-exec (first (filter (fn [[start stop]]
                                                  (between? task-id start stop))
                                                (keys executor->host+port)))
                [host port] (get executor->host+port eventlogger-exec)]
            (if (and host port)
              (doto comp-page-info
                (.set_eventlog_host host)
                (.set_eventlog_port port))))
          comp-page-info))

      (^TopologyHistoryInfo getTopologyHistory [this ^String user]
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              bases (topology-bases storm-cluster-state)
              assigned-topology-ids (.assignments storm-cluster-state nil)
              user-group-match-fn (fn [topo-id user conf]
                                    (let [topology-conf (try-read-storm-conf conf topo-id)
                                          groups (get-topo-logs-groups topology-conf)]
                                      (or (nil? user)
                                          (some #(= % user) admin-users)
                                          (does-users-group-intersect? user groups conf)
                                          (some #(= % user) (get-topo-logs-users topology-conf)))))
              active-ids-for-user (filter #(user-group-match-fn % user (:conf nimbus)) assigned-topology-ids)
              topo-history-list (read-topology-history nimbus user admin-users)]
          (TopologyHistoryInfo. (distinct (concat active-ids-for-user topo-history-list)))))

      Shutdownable
      (shutdown [this]
        (mark! nimbus:num-shutdown-calls)
        (log-message "Shutting down master")
        (cancel-timer (:timer nimbus))
        (.disconnect (:storm-cluster-state nimbus))
        (.cleanup (:downloaders nimbus))
        (.cleanup (:uploaders nimbus))
        (.close (:leader-elector nimbus))
        (if (:code-distributor nimbus) (.close (:code-distributor nimbus) (:conf nimbus)))
        (when (:nimbus-topology-action-notifier nimbus) (.cleanup (:nimbus-topology-action-notifier nimbus)))
        (log-message "Shut down master")
        )
      DaemonCommon
      (waiting? [this]
        (timer-waiting? (:timer nimbus))))))

(defmethod mk-code-distributor :distributed [conf]
  (let [code-distributor (new-instance (conf STORM-CODE-DISTRIBUTOR-CLASS))]
    (.prepare code-distributor conf)
    code-distributor))

(defmethod mk-code-distributor :local [conf]
  nil)

(defn download-code [conf nimbus storm-id host port]
  (let [tmp-root (str (master-tmp-dir conf) file-path-separator (uuid))
        storm-cluster-state (:storm-cluster-state nimbus)
        storm-root (master-stormdist-root conf storm-id)
        remote-meta-file-path (master-storm-metafile-path storm-root)
        local-meta-file-path (master-storm-metafile-path tmp-root)]
    (FileUtils/forceMkdir (File. tmp-root))
    (Utils/downloadFromHost conf remote-meta-file-path local-meta-file-path host port)
    (if (:code-distributor nimbus)
      (.download (:code-distributor nimbus) storm-id (File. local-meta-file-path)))
    (if (.exists (File. storm-root)) (FileUtils/forceDelete (File. storm-root)))
    (FileUtils/moveDirectory (File. tmp-root) (File. storm-root))
    (.setup-code-distributor! storm-cluster-state storm-id (:nimbus-host-port-info nimbus))))

(defmethod sync-code :distributed [conf nimbus]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        active-topologies (set (.code-distributor storm-cluster-state (fn [] (sync-code conf nimbus))))
        missing-topologies (set/difference active-topologies (set (code-ids (:conf nimbus))))]
    (if (not (empty? missing-topologies))
      (do
        (.removeFromLeaderLockQueue (:leader-elector nimbus))
        (doseq [missing missing-topologies]
          (log-message "missing topology " missing " has state on zookeeper but doesn't have a local dir on this host.")
          (let [nimbuses-with-missing (.code-distributor-info storm-cluster-state missing)]
            (log-message "trying to download missing topology code from " (clojure.string/join "," nimbuses-with-missing))
            (doseq [nimbus-host-port nimbuses-with-missing]
              (when-not (contains? (code-ids (:conf nimbus)) missing)
                (try
                  (download-code conf nimbus missing (.getHost nimbus-host-port) (.getPort nimbus-host-port))
                  (catch Exception e (log-error e "Exception while trying to syn-code for missing topology" missing)))))))))

    (if (empty? (set/difference active-topologies (set (code-ids (:conf nimbus)))))
      (.addToLeaderLockQueue (:leader-elector nimbus)))))

(defmethod sync-code :local [conf nimbus]
  nil)

(defn launch-server! [conf nimbus]
  (validate-distributed-mode! conf)
  (let [service-handler (service-handler conf nimbus)
        server (ThriftServer. conf (Nimbus$Processor. service-handler) 
                              ThriftConnectionType/NIMBUS)]
    (add-shutdown-hook-with-force-kill-in-1-sec (fn []
                                                  (.shutdown service-handler)
                                                  (.stop server)))
    (log-message "Starting Nimbus server...")
    (.serve server)
    service-handler))

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
  (let [conf (merge
               (read-storm-config)
               (read-yaml-config "storm-cluster-auth.yaml" false))]
  (launch-server! conf nimbus)))

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
  (setup-default-uncaught-exception-handler)
  (-launch (standalone-nimbus)))
