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
(ns org.apache.storm.nimbus-test
  (:use [clojure test])
  (:require [org.apache.storm [util :as util]])
  (:require [org.apache.storm.daemon [nimbus :as nimbus]])
  (:require [org.apache.storm [converter :as converter]])
  (:import [org.apache.storm.testing TestWordCounter TestWordSpout TestGlobalCount
            TestAggregatesCounter TestPlannerSpout TestPlannerBolt]
           [org.apache.storm.nimbus InMemoryTopologyActionNotifier]
           [org.apache.storm.generated GlobalStreamId]
           [org.apache.storm Thrift MockAutoCred]
           [org.apache.storm.stats BoltExecutorStats StatsUtil])
  (:import [org.apache.storm.testing.staticmocking MockedZookeeper])
  (:import [org.apache.storm.scheduler INimbus])
  (:import [org.mockito Mockito Matchers])
  (:import [org.mockito.exceptions.base MockitoAssertionError])
  (:import [org.apache.storm.nimbus ILeaderElector NimbusInfo])
  (:import [org.apache.storm.testing.staticmocking MockedCluster])
  (:import [org.apache.storm.generated Credentials NotAliveException SubmitOptions
            TopologyInitialStatus TopologyStatus AlreadyAliveException KillOptions RebalanceOptions
            InvalidTopologyException AuthorizationException
            LogConfig LogLevel LogLevelAction])
  (:import [java.util HashMap])
  (:import [java.io File])
  (:import [org.apache.storm.utils Time Utils Utils$UptimeComputer ConfigUtils IPredicate StormCommonInstaller]
           [org.apache.storm.utils.staticmocking ConfigUtilsInstaller UtilsInstaller])
  (:import [org.apache.storm.zookeeper Zookeeper])
  (:import [org.apache.commons.io FileUtils])
  (:import [org.json.simple JSONValue])
  (:import [org.apache.storm.daemon StormCommon])
  (:import [org.apache.storm.cluster IStormClusterState StormClusterStateImpl ClusterStateContext ClusterUtils])
  (:use [org.apache.storm testing util config log converter])
    (:require [conjure.core] [org.apache.storm.daemon.worker :as worker])

  (:use [conjure core]))

(defn- from-json
       [^String str]
       (if str
         (clojurify-structure
           (JSONValue/parse str))
         nil))

(defn storm-component->task-info [cluster storm-name]
  (let [storm-id (StormCommon/getStormId (:storm-cluster-state cluster) storm-name)
        nimbus (:nimbus cluster)]
    (-> (.getUserTopology nimbus storm-id)
        (#(StormCommon/stormTaskInfo % (from-json (.getTopologyConf nimbus storm-id))))
        (Utils/reverseMap)
        clojurify-structure)))

(defn getCredentials [cluster storm-name]
  (let [storm-id (StormCommon/getStormId (:storm-cluster-state cluster) storm-name)]
    (clojurify-crdentials (.credentials (:storm-cluster-state cluster) storm-id nil))))

(defn storm-component->executor-info [cluster storm-name]
  (let [storm-id (StormCommon/getStormId (:storm-cluster-state cluster) storm-name)
        nimbus (:nimbus cluster)
        storm-conf (from-json (.getTopologyConf nimbus storm-id))
        topology (.getUserTopology nimbus storm-id)
        task->component (clojurify-structure (StormCommon/stormTaskInfo topology storm-conf))
        state (:storm-cluster-state cluster)
        get-component (comp task->component first)]
    (->> (clojurify-assignment (.assignmentInfo state storm-id nil))
         :executor->node+port
         keys
         (map (fn [e] {e (get-component e)}))
         (apply merge)
         (Utils/reverseMap)
         clojurify-structure)))

(defn storm-num-workers [state storm-name]
  (let [storm-id (StormCommon/getStormId state storm-name)
        assignment (clojurify-assignment (.assignmentInfo state storm-id nil))]
    (count (clojurify-structure (Utils/reverseMap (:executor->node+port assignment))))
    ))

(defn topology-nodes [state storm-name]
  (let [storm-id (StormCommon/getStormId state storm-name)
        assignment (clojurify-assignment (.assignmentInfo state storm-id nil))]
    (->> assignment
         :executor->node+port
         vals
         (map first)
         set         
         )))

(defn topology-slots [state storm-name]
  (let [storm-id (StormCommon/getStormId state storm-name)
        assignment (clojurify-assignment (.assignmentInfo state storm-id nil))]
    (->> assignment
         :executor->node+port
         vals
         set         
         )))

;TODO: when translating this function, don't call map-val, but instead use an inline for loop.
; map-val is a temporary kluge for clojure.
(defn topology-node-distribution [state storm-name]
  (let [storm-id (StormCommon/getStormId state storm-name)
        assignment (clojurify-assignment (.assignmentInfo state storm-id nil))]
    (->> assignment
         :executor->node+port
         vals
         set
         (group-by first)
         (map-val count)
         (map (fn [[_ amt]] {amt 1}))
         (apply merge-with +)       
         )))

(defn topology-num-nodes [state storm-name]
  (count (topology-nodes state storm-name)))

(defn executor-assignment [cluster storm-id executor-id]
  (let [state (:storm-cluster-state cluster)
        assignment (clojurify-assignment (.assignmentInfo state storm-id nil))]
    ((:executor->node+port assignment) executor-id)
    ))

(defn executor-start-times [cluster storm-id]
  (let [state (:storm-cluster-state cluster)
        assignment (clojurify-assignment (.assignmentInfo state storm-id nil))]
    (:executor->start-time-secs assignment)))

(defn do-executor-heartbeat [cluster storm-id executor]
  (let [state (:storm-cluster-state cluster)
        executor->node+port (:executor->node+port (clojurify-assignment (.assignmentInfo state storm-id nil)))
        [node port] (get executor->node+port executor)
        curr-beat (StatsUtil/convertZkWorkerHb (.getWorkerHeartbeat state storm-id node port))
        stats (if (get curr-beat "executor-stats")
                (get curr-beat "executor-stats")
                (HashMap.))]
    (log-warn "curr-beat:" (prn-str curr-beat) ",stats:" (prn-str stats))
    (log-warn "stats type:" (type stats))
    (.put stats (StatsUtil/convertExecutor executor) (.renderStats (BoltExecutorStats. 20)))
    (log-warn "merged:" stats)

    (.workerHeartbeat state storm-id node port
      (StatsUtil/thriftifyZkWorkerHb (StatsUtil/mkZkWorkerHb storm-id stats (int 10))))))

(defn slot-assignments [cluster storm-id]
  (let [state (:storm-cluster-state cluster)
        assignment (clojurify-assignment (.assignmentInfo state storm-id nil))]
        (clojurify-structure (Utils/reverseMap (:executor->node+port assignment)))))

(defn task-ids [cluster storm-id]
  (let [nimbus (:nimbus cluster)]
    (-> (.getUserTopology nimbus storm-id)
        (#(StormCommon/stormTaskInfo % (from-json (.getTopologyConf nimbus storm-id))))
        clojurify-structure
        keys)))

(defn topology-executors [cluster storm-id]
  (let [state (:storm-cluster-state cluster)
        assignment (clojurify-assignment (.assignmentInfo state storm-id nil))
    ret-keys (keys (:executor->node+port assignment))
        _ (log-message "ret-keys: " (pr-str ret-keys)) ]
    ret-keys
    ))

(defn check-distribution [items distribution]
  (let [counts (map count items)]
    (is (ms= counts distribution))))

(defn disjoint? [& sets]
  (let [combined (apply concat sets)]
    (= (count combined) (count (set combined)))
    ))

(defn executor->tasks [executor-id]
  clojurify-structure (StormCommon/executorIdToTasks executor-id))

(defnk check-consistency [cluster storm-name :assigned? true]
  (let [state (:storm-cluster-state cluster)
        storm-id (StormCommon/getStormId state storm-name)
        task-ids (task-ids cluster storm-id)
        assignment (clojurify-assignment (.assignmentInfo state storm-id nil))
        executor->node+port (:executor->node+port assignment)
        task->node+port (worker/task->node_port executor->node+port)
        assigned-task-ids (mapcat executor->tasks (keys executor->node+port))
        all-nodes (set (map first (vals executor->node+port)))]
    (when assigned?
      (is (= (sort task-ids) (sort assigned-task-ids)))
      (doseq [t task-ids]
        (is (not-nil? (task->node+port t)))))
    (doseq [[e s] executor->node+port]
      (is (not-nil? s)))
    
    ;;(map str (-> (Thread/currentThread) .getStackTrace))
    (is (= all-nodes (set (keys (:node->host assignment)))))
    (doseq [[e s] executor->node+port]
      (is (not-nil? ((:executor->start-time-secs assignment) e))))
    ))

(deftest test-bogusId
  (with-local-cluster [cluster :supervisors 4 :ports-per-supervisor 3
                       :daemon-conf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0 TOPOLOGY-EVENTLOGGER-EXECUTORS 0}]
    (let [state (:storm-cluster-state cluster)
          nimbus (:nimbus cluster)]
       (is (thrown? NotAliveException (.getTopologyConf nimbus "bogus-id")))
       (is (thrown? NotAliveException (.getTopology nimbus "bogus-id")))
       (is (thrown? NotAliveException (.getUserTopology nimbus "bogus-id")))
       (is (thrown? NotAliveException (.getTopologyInfo nimbus "bogus-id")))
       (is (thrown? NotAliveException (.uploadNewCredentials nimbus "bogus-id" (Credentials.))))
      )))

(deftest test-assignment
  (with-simulated-time-local-cluster [cluster :supervisors 4 :ports-per-supervisor 3
                       :daemon-conf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0 TOPOLOGY-EVENTLOGGER-EXECUTORS 0}]
    (let [state (:storm-cluster-state cluster)
          nimbus (:nimbus cluster)
          topology (Thrift/buildTopology
                     {"1" (Thrift/prepareSpoutDetails
                            (TestPlannerSpout. false) (Integer. 3))}
                     {"2" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareNoneGrouping)}
                            (TestPlannerBolt.) (Integer. 4))
                      "3" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "2" nil)
                             (Thrift/prepareNoneGrouping)}
                            (TestPlannerBolt.))})
          topology2 (Thrift/buildTopology
                      {"1" (Thrift/prepareSpoutDetails
                             (TestPlannerSpout. true) (Integer. 12))}
                      {"2" (Thrift/prepareBoltDetails
                             {(Utils/getGlobalStreamId "1" nil)
                              (Thrift/prepareNoneGrouping)}
                             (TestPlannerBolt.) (Integer. 6))
                       "3" (Thrift/prepareBoltDetails
                             {(Utils/getGlobalStreamId "1" nil)
                              (Thrift/prepareGlobalGrouping)}
                             (TestPlannerBolt.) (Integer. 8))
                       "4" (Thrift/prepareBoltDetails
                             {(Utils/getGlobalStreamId "1" nil)
                              (Thrift/prepareGlobalGrouping)
                              (Utils/getGlobalStreamId "2" nil)
                              (Thrift/prepareNoneGrouping)}
                             (TestPlannerBolt.) (Integer. 4))})
          _ (submit-local-topology nimbus "mystorm" {TOPOLOGY-WORKERS 4} topology)
          _ (advance-cluster-time cluster 11)
          task-info (storm-component->task-info cluster "mystorm")]
      (check-consistency cluster "mystorm")
      ;; 3 should be assigned once (if it were optimized, we'd have
      ;; different topology)
      (is (= 1 (count (.assignments state nil))))
      (is (= 1 (count (task-info "1"))))
      (is (= 4 (count (task-info "2"))))
      (is (= 1 (count (task-info "3"))))
      (is (= 4 (storm-num-workers state "mystorm")))
      (submit-local-topology nimbus "storm2" {TOPOLOGY-WORKERS 20} topology2)
      (advance-cluster-time cluster 11)
      (check-consistency cluster "storm2")
      (is (= 2 (count (.assignments state nil))))
      (let [task-info (storm-component->task-info cluster "storm2")]
        (is (= 12 (count (task-info "1"))))
        (is (= 6 (count (task-info "2"))))
        (is (= 8 (count (task-info "3"))))
        (is (= 4 (count (task-info "4"))))
        (is (= 8 (storm-num-workers state "storm2")))
        )
      )))

(defn isolation-nimbus []
  (let [standalone (nimbus/standalone-nimbus)]
    (reify INimbus
      (prepare [this conf local-dir]
        (.prepare standalone conf local-dir)
        )
      (allSlotsAvailableForScheduling [this supervisors topologies topologies-missing-assignments]
        (.allSlotsAvailableForScheduling standalone supervisors topologies topologies-missing-assignments))
      (assignSlots [this topology slots]
        (.assignSlots standalone topology slots)
        )
      (getForcedScheduler [this]
        (.getForcedScheduler standalone))
      (getHostName [this supervisors node-id]
        node-id
      ))))


(deftest test-auto-credentials
  (with-simulated-time-local-cluster [cluster :supervisors 6
                                      :ports-per-supervisor 3
                                      :daemon-conf {SUPERVISOR-ENABLE false
                                                    TOPOLOGY-ACKER-EXECUTORS 0
                                                    TOPOLOGY-EVENTLOGGER-EXECUTORS 0
                                                    NIMBUS-CREDENTIAL-RENEW-FREQ-SECS 10
                                                    NIMBUS-CREDENTIAL-RENEWERS (list "org.apache.storm.MockAutoCred")
                                                    NIMBUS-AUTO-CRED-PLUGINS (list "org.apache.storm.MockAutoCred")
                                                    }]
    (let [state (:storm-cluster-state cluster)
          nimbus (:nimbus cluster)
          topology-name "test-auto-cred-storm"
          submitOptions (SubmitOptions. TopologyInitialStatus/INACTIVE)
          - (.set_creds submitOptions (Credentials. (HashMap.)))
          topology (Thrift/buildTopology
                     {"1" (Thrift/prepareSpoutDetails
                            (TestPlannerSpout. false) (Integer. 3))}
                     {"2" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareNoneGrouping)}
                            (TestPlannerBolt.) (Integer. 4))
                      "3" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "2" nil)
                             (Thrift/prepareNoneGrouping)}
                            (TestPlannerBolt.))})
          _ (submit-local-topology-with-opts nimbus topology-name {TOPOLOGY-WORKERS 4
                                                               TOPOLOGY-AUTO-CREDENTIALS (list "org.apache.storm.MockAutoCred")
                                                               } topology submitOptions)
          credentials (getCredentials cluster topology-name)]
      ; check that the credentials have nimbus auto generated cred
      (is (= (.get credentials MockAutoCred/NIMBUS_CRED_KEY) MockAutoCred/NIMBUS_CRED_VAL))
      ;advance cluster time so the renewers can execute
      (advance-cluster-time cluster 20)
      ;check that renewed credentials replace the original credential.
      (is (= (.get (getCredentials cluster topology-name) MockAutoCred/NIMBUS_CRED_KEY) MockAutoCred/NIMBUS_CRED_RENEW_VAL))
      (is (= (.get (getCredentials cluster topology-name) MockAutoCred/GATEWAY_CRED_KEY) MockAutoCred/GATEWAY_CRED_RENEW_VAL)))))

(defmacro letlocals
  [& body]
  (let [[tobind lexpr] (split-at (dec (count body)) body)
        binded (vec (mapcat (fn [e]
                              (if (and (list? e) (= 'bind (first e)))
                                [(second e) (last e)]
                                ['_ e]
                                ))
                            tobind))]
    `(let ~binded
       ~(first lexpr))))

(deftest test-isolated-assignment
  (with-simulated-time-local-cluster [cluster :supervisors 6
                               :ports-per-supervisor 3
                               :inimbus (isolation-nimbus)
                               :daemon-conf {SUPERVISOR-ENABLE false
                                             TOPOLOGY-ACKER-EXECUTORS 0
                                             TOPOLOGY-EVENTLOGGER-EXECUTORS 0
                                             STORM-SCHEDULER "org.apache.storm.scheduler.IsolationScheduler"
                                             ISOLATION-SCHEDULER-MACHINES {"tester1" 3 "tester2" 2}
                                             NIMBUS-MONITOR-FREQ-SECS 10
                                             }]
    (letlocals
      (bind state (:storm-cluster-state cluster))
      (bind nimbus (:nimbus cluster))
      (bind topology (Thrift/buildTopology
                      {"1" (Thrift/prepareSpoutDetails
                             (TestPlannerSpout. false) (Integer. 3))}
                      {"2" (Thrift/prepareBoltDetails
                             {(Utils/getGlobalStreamId "1" nil)
                              (Thrift/prepareNoneGrouping)}
                             (TestPlannerBolt.) (Integer. 5))
                       "3" (Thrift/prepareBoltDetails
                             {(Utils/getGlobalStreamId "2" nil)
                              (Thrift/prepareNoneGrouping)}
                             (TestPlannerBolt.))}))

      (submit-local-topology nimbus "noniso" {TOPOLOGY-WORKERS 4} topology)
      (advance-cluster-time cluster 11)
      (is (= 4 (topology-num-nodes state "noniso")))
      (is (= 4 (storm-num-workers state "noniso")))

      (submit-local-topology nimbus "tester1" {TOPOLOGY-WORKERS 6} topology)
      (submit-local-topology nimbus "tester2" {TOPOLOGY-WORKERS 6} topology)
      (advance-cluster-time cluster 11)

      (bind task-info-tester1 (storm-component->task-info cluster "tester1"))
      (bind task-info-tester2 (storm-component->task-info cluster "tester2"))


      (is (= 1 (topology-num-nodes state "noniso")))
      (is (= 3 (storm-num-workers state "noniso")))

      (is (= {2 3} (topology-node-distribution state "tester1")))
      (is (= {3 2} (topology-node-distribution state "tester2")))

      (is (apply disjoint? (map (partial topology-nodes state) ["noniso" "tester1" "tester2"])))

      (check-consistency cluster "tester1")
      (check-consistency cluster "tester2")
      (check-consistency cluster "noniso")

      ;;check that nothing gets reassigned
      (bind tester1-slots (topology-slots state "tester1"))
      (bind tester2-slots (topology-slots state "tester2"))
      (bind noniso-slots (topology-slots state "noniso"))
      (advance-cluster-time cluster 20)
      (is (= tester1-slots (topology-slots state "tester1")))
      (is (= tester2-slots (topology-slots state "tester2")))
      (is (= noniso-slots (topology-slots state "noniso")))

      )))

(deftest test-zero-executor-or-tasks
  (with-simulated-time-local-cluster [cluster :daemon-conf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0 TOPOLOGY-EVENTLOGGER-EXECUTORS 0}]
    (let [state (:storm-cluster-state cluster)
          nimbus (:nimbus cluster)
          topology (Thrift/buildTopology
                    {"1" (Thrift/prepareSpoutDetails
                           (TestPlannerSpout. false) (Integer. 3)
                           {TOPOLOGY-TASKS 0})}
                    {"2" (Thrift/prepareBoltDetails
                           {(Utils/getGlobalStreamId "1" nil)
                            (Thrift/prepareNoneGrouping)}
                           (TestPlannerBolt.) (Integer. 1)
                           {TOPOLOGY-TASKS 2})
                     "3" (Thrift/prepareBoltDetails
                           {(Utils/getGlobalStreamId "2" nil)
                            (Thrift/prepareNoneGrouping)}
                           (TestPlannerBolt.) nil
                           {TOPOLOGY-TASKS 5})})
          _ (submit-local-topology nimbus "mystorm" {TOPOLOGY-WORKERS 4} topology)
          _ (advance-cluster-time cluster 11)
          task-info (storm-component->task-info cluster "mystorm")]
      (check-consistency cluster "mystorm")
      (is (= 0 (count (task-info "1"))))
      (is (= 2 (count (task-info "2"))))
      (is (= 5 (count (task-info "3"))))
      (is (= 2 (storm-num-workers state "mystorm"))) ;; because only 2 executors
      )))

;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
(deftest test-executor-assignments
  (with-simulated-time-local-cluster[cluster :daemon-conf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0 TOPOLOGY-EVENTLOGGER-EXECUTORS 0}]
    (let [nimbus (:nimbus cluster)
          topology (Thrift/buildTopology
                    {"1" (Thrift/prepareSpoutDetails
                           (TestPlannerSpout. true) (Integer. 3)
                           {TOPOLOGY-TASKS 5})}
                    {"2" (Thrift/prepareBoltDetails
                           {(Utils/getGlobalStreamId "1" nil)
                            (Thrift/prepareNoneGrouping)}
                           (TestPlannerBolt.) (Integer. 8)
                           {TOPOLOGY-TASKS 2})
                     "3" (Thrift/prepareBoltDetails
                           {(Utils/getGlobalStreamId "2" nil)
                            (Thrift/prepareNoneGrouping)}
                           (TestPlannerBolt.) (Integer. 3))})
          _ (submit-local-topology nimbus "mystorm" {TOPOLOGY-WORKERS 4} topology)
          _ (advance-cluster-time cluster 11)
          task-info (storm-component->task-info cluster "mystorm")
          executor-info (->> (storm-component->executor-info cluster "mystorm")
                             (map-val #(map executor->tasks %)))]
      (check-consistency cluster "mystorm")
      (is (= 5 (count (task-info "1"))))
      (check-distribution (executor-info "1") [2 2 1])

      (is (= 2 (count (task-info "2"))))
      (check-distribution (executor-info "2") [1 1])

      (is (= 3 (count (task-info "3"))))
      (check-distribution (executor-info "3") [1 1 1])
      )))

(deftest test-over-parallelism-assignment
  (with-simulated-time-local-cluster [cluster :supervisors 2 :ports-per-supervisor 5
                       :daemon-conf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0 TOPOLOGY-EVENTLOGGER-EXECUTORS 0}]
    (let [state (:storm-cluster-state cluster)
          nimbus (:nimbus cluster)
          topology (Thrift/buildTopology
                     {"1" (Thrift/prepareSpoutDetails
                            (TestPlannerSpout. true) (Integer. 21))}
                     {"2" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareNoneGrouping)}
                            (TestPlannerBolt.) (Integer. 9))
                      "3" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareNoneGrouping)}
                            (TestPlannerBolt.) (Integer. 2))
                      "4" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareNoneGrouping)}
                            (TestPlannerBolt.) (Integer. 10))})
          _ (submit-local-topology nimbus "test" {TOPOLOGY-WORKERS 7} topology)
          _ (advance-cluster-time cluster 11)
          task-info (storm-component->task-info cluster "test")]
      (check-consistency cluster "test")
      (is (= 21 (count (task-info "1"))))
      (is (= 9 (count (task-info "2"))))
      (is (= 2 (count (task-info "3"))))
      (is (= 10 (count (task-info "4"))))
      (is (= 7 (storm-num-workers state "test")))
    )))

(deftest test-topo-history
  (with-simulated-time-local-cluster [cluster :supervisors 2 :ports-per-supervisor 5
                                      :daemon-conf {SUPERVISOR-ENABLE false
                                                    NIMBUS-ADMINS ["admin-user"]
                                                    NIMBUS-TASK-TIMEOUT-SECS 30
                                                    NIMBUS-MONITOR-FREQ-SECS 10
                                                    TOPOLOGY-ACKER-EXECUTORS 0}]

    (stubbing [nimbus/user-groups ["alice-group"]]
      (letlocals
        (bind conf (:daemon-conf cluster))
        (bind topology (Thrift/buildTopology
                         {"1" (Thrift/prepareSpoutDetails
                                (TestPlannerSpout. true) (Integer. 4))}
                         {}))
        (bind state (:storm-cluster-state cluster))
        (submit-local-topology (:nimbus cluster) "test" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 20, LOGS-USERS ["alice", (System/getProperty "user.name")]} topology)
        (bind storm-id (StormCommon/getStormId state "test"))
        (advance-cluster-time cluster 5)
        (is (not-nil? (clojurify-storm-base (.stormBase state storm-id nil))))
        (is (not-nil? (clojurify-assignment (.assignmentInfo state storm-id nil))))
        (.killTopology (:nimbus cluster) "test")
        ;; check that storm is deactivated but alive
        (is (= :killed (-> (clojurify-storm-base (.stormBase state storm-id nil)) :status :type)))
        (is (not-nil? (clojurify-assignment (.assignmentInfo state storm-id nil))))
        (advance-cluster-time cluster 35)
        ;; kill topology read on group
        (submit-local-topology (:nimbus cluster) "killgrouptest" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 20, LOGS-GROUPS ["alice-group"]} topology)
        (bind storm-id-killgroup (StormCommon/getStormId state "killgrouptest"))
        (advance-cluster-time cluster 5)
        (is (not-nil? (clojurify-storm-base (.stormBase state storm-id-killgroup nil))))
        (is (not-nil? (clojurify-assignment (.assignmentInfo state storm-id-killgroup nil))))
        (.killTopology (:nimbus cluster) "killgrouptest")
        ;; check that storm is deactivated but alive
        (is (= :killed (-> (clojurify-storm-base (.stormBase state storm-id-killgroup nil)) :status :type)))
        (is (not-nil? (clojurify-assignment (.assignmentInfo state storm-id-killgroup nil))))
        (advance-cluster-time cluster 35)
        ;; kill topology can't read
        (submit-local-topology (:nimbus cluster) "killnoreadtest" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 20} topology)
        (bind storm-id-killnoread (StormCommon/getStormId state "killnoreadtest"))
        (advance-cluster-time cluster 5)
        (is (not-nil? (clojurify-storm-base (.stormBase state storm-id-killnoread nil))))
        (is (not-nil? (clojurify-assignment (.assignmentInfo state storm-id-killnoread nil))))
        (.killTopology (:nimbus cluster) "killnoreadtest")
        ;; check that storm is deactivated but alive
        (is (= :killed (-> (clojurify-storm-base (.stormBase state storm-id-killnoread nil)) :status :type)))
        (is (not-nil? (clojurify-assignment (.assignmentInfo state storm-id-killnoread nil))))
        (advance-cluster-time cluster 35)

        ;; active topology can read
        (submit-local-topology (:nimbus cluster) "2test" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 10, LOGS-USERS ["alice", (System/getProperty "user.name")]} topology)
        (advance-cluster-time cluster 11)
        (bind storm-id2 (StormCommon/getStormId state "2test"))
        (is (not-nil? (clojurify-storm-base (.stormBase state storm-id2 nil))))
        (is (not-nil? (clojurify-assignment (.assignmentInfo state storm-id2 nil))))
        ;; active topology can not read
        (submit-local-topology (:nimbus cluster) "testnoread" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 10, LOGS-USERS ["alice"]} topology)
        (advance-cluster-time cluster 11)
        (bind storm-id3 (StormCommon/getStormId state "testnoread"))
        (is (not-nil? (clojurify-storm-base (.stormBase state storm-id3 nil))))
        (is (not-nil? (clojurify-assignment (.assignmentInfo state storm-id3 nil))))
        ;; active topology can read based on group
        (submit-local-topology (:nimbus cluster) "testreadgroup" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 10, LOGS-GROUPS ["alice-group"]} topology)
        (advance-cluster-time cluster 11)
        (bind storm-id4 (StormCommon/getStormId state "testreadgroup"))
        (is (not-nil? (clojurify-storm-base (.stormBase state storm-id4 nil))))
        (is (not-nil? (clojurify-assignment (.assignmentInfo state storm-id4 nil))))
        ;; at this point have 1 running, 1 killed topo
        (let [hist-topo-ids (vec (sort (.get_topo_ids (.getTopologyHistory (:nimbus cluster) (System/getProperty "user.name")))))]
          (log-message "Checking user " (System/getProperty "user.name") " " hist-topo-ids)
          (is (= 4 (count hist-topo-ids)))
          (is (= storm-id2 (get hist-topo-ids 0)))
          (is (= storm-id-killgroup (get hist-topo-ids 1)))
          (is (= storm-id (get hist-topo-ids 2)))
          (is (= storm-id4 (get hist-topo-ids 3))))
        (let [hist-topo-ids (vec (sort (.get_topo_ids (.getTopologyHistory (:nimbus cluster) "alice"))))]
          (log-message "Checking user alice " hist-topo-ids)
          (is (= 5 (count hist-topo-ids)))
          (is (= storm-id2 (get hist-topo-ids 0)))
          (is (= storm-id-killgroup (get hist-topo-ids 1)))
          (is (= storm-id (get hist-topo-ids 2)))
          (is (= storm-id3 (get hist-topo-ids 3)))
          (is (= storm-id4 (get hist-topo-ids 4))))
        (let [hist-topo-ids (vec (sort (.get_topo_ids (.getTopologyHistory (:nimbus cluster) "admin-user"))))]
          (log-message "Checking user admin-user " hist-topo-ids)
          (is (= 6 (count hist-topo-ids)))
          (is (= storm-id2 (get hist-topo-ids 0)))
          (is (= storm-id-killgroup (get hist-topo-ids 1)))
          (is (= storm-id-killnoread (get hist-topo-ids 2)))
          (is (= storm-id (get hist-topo-ids 3)))
          (is (= storm-id3 (get hist-topo-ids 4)))
          (is (= storm-id4 (get hist-topo-ids 5))))
        (let [hist-topo-ids (vec (sort (.get_topo_ids (.getTopologyHistory (:nimbus cluster) "group-only-user"))))]
          (log-message "Checking user group-only-user " hist-topo-ids)
          (is (= 2 (count hist-topo-ids)))
          (is (= storm-id-killgroup (get hist-topo-ids 0)))
          (is (= storm-id4 (get hist-topo-ids 1))))))))

(deftest test-kill-storm
  (with-simulated-time-local-cluster [cluster :supervisors 2 :ports-per-supervisor 5
    :daemon-conf {SUPERVISOR-ENABLE false
                  NIMBUS-TASK-TIMEOUT-SECS 30
                  NIMBUS-MONITOR-FREQ-SECS 10
                  TOPOLOGY-ACKER-EXECUTORS 0
                  TOPOLOGY-EVENTLOGGER-EXECUTORS 0}]
    (letlocals
      (bind conf (:daemon-conf cluster))
      (bind topology (Thrift/buildTopology
                       {"1" (Thrift/prepareSpoutDetails
                              (TestPlannerSpout. true) (Integer. 14))}
                       {}))
      (bind state (:storm-cluster-state cluster))
      (submit-local-topology (:nimbus cluster) "test" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 20} topology)
      (bind storm-id (StormCommon/getStormId state "test"))
      (advance-cluster-time cluster 15)
      (is (not-nil? (clojurify-storm-base (.stormBase state storm-id nil))))
      (is (not-nil? (clojurify-assignment (.assignmentInfo state storm-id nil))))
      (.killTopology (:nimbus cluster) "test")
      ;; check that storm is deactivated but alive
      (is (= :killed (-> (clojurify-storm-base (.stormBase state storm-id nil)) :status :type)))
      (is (not-nil? (clojurify-assignment (.assignmentInfo state storm-id nil))))
      (advance-cluster-time cluster 18)
      ;; check that storm is deactivated but alive
      (is (= 1 (count (.heartbeatStorms state))))
      (advance-cluster-time cluster 3)
      (is (nil? (clojurify-storm-base (.stormBase state storm-id nil))))
      (is (nil? (clojurify-assignment (.assignmentInfo state storm-id nil))))

      ;; cleanup happens on monitoring thread
      (advance-cluster-time cluster 11)
      (is (empty? (.heartbeatStorms state)))
      ;; TODO: check that code on nimbus was cleaned up locally...

      (is (thrown? NotAliveException (.killTopology (:nimbus cluster) "lalala")))
      (submit-local-topology (:nimbus cluster) "2test" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 10} topology)
      (advance-cluster-time cluster 11)
      (is (thrown? AlreadyAliveException (submit-local-topology (:nimbus cluster) "2test" {} topology)))
      (advance-cluster-time cluster 11)
      (bind storm-id (StormCommon/getStormId state "2test"))
      (is (not-nil? (clojurify-storm-base (.stormBase state storm-id nil))))
      (.killTopology (:nimbus cluster) "2test")
      (is (thrown? AlreadyAliveException (submit-local-topology (:nimbus cluster) "2test" {} topology)))
      (advance-cluster-time cluster 11)
      (is (= 1 (count (.heartbeatStorms state))))

      (advance-cluster-time cluster 6)
      (is (nil? (clojurify-storm-base (.stormBase state storm-id nil))))
      (is (nil? (clojurify-assignment (.assignmentInfo state storm-id nil))))
      (advance-cluster-time cluster 11)
      (is (= 0 (count (.heartbeatStorms state))))

      (submit-local-topology (:nimbus cluster) "test3" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 5} topology)
      (bind storm-id3 (StormCommon/getStormId state "test3"))
      (advance-cluster-time cluster 11)
      (.removeStorm state storm-id3)
      (is (nil? (clojurify-storm-base (.stormBase state storm-id3 nil))))
      (is (nil? (clojurify-assignment (.assignmentInfo state storm-id3 nil))))

      (advance-cluster-time cluster 11)
      (is (= 0 (count (.heartbeatStorms state))))

      ;; this guarantees that monitor thread won't trigger for 10 more seconds
      (advance-time-secs! 11)
      (wait-until-cluster-waiting cluster)

      (submit-local-topology (:nimbus cluster) "test3" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 5} topology)
      (bind storm-id3 (StormCommon/getStormId state "test3"))

      (advance-cluster-time cluster 11)
      (bind executor-id (first (topology-executors cluster storm-id3)))

      (do-executor-heartbeat cluster storm-id3 executor-id)

      (.killTopology (:nimbus cluster) "test3")
      (advance-cluster-time cluster 6)
      (is (= 1 (count (.heartbeatStorms state))))
      (advance-cluster-time cluster 5)
      (is (= 0 (count (.heartbeatStorms state))))

      ;; test kill with opts
      (submit-local-topology (:nimbus cluster) "test4" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 100} topology)
      (advance-cluster-time cluster 11)
      (.killTopologyWithOpts (:nimbus cluster) "test4" (doto (KillOptions.) (.set_wait_secs 10)))
      (bind storm-id4 (StormCommon/getStormId state "test4"))
      (advance-cluster-time cluster 9)
      (is (not-nil? (clojurify-assignment (.assignmentInfo state storm-id4 nil))))
      (advance-cluster-time cluster 2)
      (is (nil? (clojurify-assignment (.assignmentInfo state storm-id4 nil))))
      )))

(deftest test-reassignment
  (with-simulated-time-local-cluster [cluster :supervisors 2 :ports-per-supervisor 5
    :daemon-conf {SUPERVISOR-ENABLE false
                  NIMBUS-TASK-LAUNCH-SECS 60
                  NIMBUS-TASK-TIMEOUT-SECS 20
                  NIMBUS-MONITOR-FREQ-SECS 10
                  NIMBUS-SUPERVISOR-TIMEOUT-SECS 100
                  TOPOLOGY-ACKER-EXECUTORS 0
                  TOPOLOGY-EVENTLOGGER-EXECUTORS 0}]
    (letlocals
      (bind conf (:daemon-conf cluster))
      (bind topology (Thrift/buildTopology
                       {"1" (Thrift/prepareSpoutDetails
                              (TestPlannerSpout. true) (Integer. 2))}
                       {}))
      (bind state (:storm-cluster-state cluster))
      (submit-local-topology (:nimbus cluster) "test" {TOPOLOGY-WORKERS 2} topology)
      (advance-cluster-time cluster 11)
      (check-consistency cluster "test")
      (bind storm-id (StormCommon/getStormId state "test"))
      (bind [executor-id1 executor-id2]  (topology-executors cluster storm-id))
      (bind ass1 (executor-assignment cluster storm-id executor-id1))
      (bind ass2 (executor-assignment cluster storm-id executor-id2))
      (bind _ (log-message "ass1, t0: " (pr-str ass1)))
      (bind _ (log-message "ass2, t0: " (pr-str ass2)))

      (advance-cluster-time cluster 30)
      (bind _ (log-message "ass1, t30, pre beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t30, pre beat: " (pr-str ass2)))
      (do-executor-heartbeat cluster storm-id executor-id1)
      (do-executor-heartbeat cluster storm-id executor-id2)
      (bind _ (log-message "ass1, t30, post beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t30, post beat: " (pr-str ass2)))

      (advance-cluster-time cluster 13)
      (bind _ (log-message "ass1, t43, pre beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t43, pre beat: " (pr-str ass2)))
      (is (= ass1 (executor-assignment cluster storm-id executor-id1)))
      (is (= ass2 (executor-assignment cluster storm-id executor-id2)))
      (do-executor-heartbeat cluster storm-id executor-id1)
      (bind _ (log-message "ass1, t43, post beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t43, post beat: " (pr-str ass2)))

      (advance-cluster-time cluster 11)
      (bind _ (log-message "ass1, t54, pre beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t54, pre beat: " (pr-str ass2)))
      (do-executor-heartbeat cluster storm-id executor-id1)
      (bind _ (log-message "ass1, t54, post beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t54, post beat: " (pr-str ass2)))
      (is (= ass1 (executor-assignment cluster storm-id executor-id1)))
      (check-consistency cluster "test")

      ; have to wait an extra 10 seconds because nimbus may not
      ; resynchronize its heartbeat time till monitor-time secs after
      (advance-cluster-time cluster 11)
      (bind _ (log-message "ass1, t65, pre beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t65, pre beat: " (pr-str ass2)))
      (do-executor-heartbeat cluster storm-id executor-id1)
      (bind _ (log-message "ass1, t65, post beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t65, post beat: " (pr-str ass2)))
      (is (= ass1 (executor-assignment cluster storm-id executor-id1)))
      (check-consistency cluster "test")

      (advance-cluster-time cluster 11)
      (bind _ (log-message "ass1, t76, pre beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t76, pre beat: " (pr-str ass2)))
      (is (= ass1 (executor-assignment cluster storm-id executor-id1)))
      (is (not= ass2 (executor-assignment cluster storm-id executor-id2)))
      (bind ass2 (executor-assignment cluster storm-id executor-id2))
      (bind _ (log-message "ass1, t76, post beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t76, post beat: " (pr-str ass2)))
      (check-consistency cluster "test")

      (advance-cluster-time cluster 31)
      (is (not= ass1 (executor-assignment cluster storm-id executor-id1)))
      (is (= ass2 (executor-assignment cluster storm-id executor-id2)))  ; tests launch timeout
      (check-consistency cluster "test")


      (bind ass1 (executor-assignment cluster storm-id executor-id1))
      (bind active-supervisor (first ass2))
      (kill-supervisor cluster active-supervisor)

      (doseq [i (range 12)]
        (do-executor-heartbeat cluster storm-id executor-id1)
        (do-executor-heartbeat cluster storm-id executor-id2)
        (advance-cluster-time cluster 10)
        )
      ;; tests that it doesn't reassign executors if they're heartbeating even if supervisor times out
      (is (= ass1 (executor-assignment cluster storm-id executor-id1)))
      (is (= ass2 (executor-assignment cluster storm-id executor-id2)))
      (check-consistency cluster "test")

      (advance-cluster-time cluster 30)

      (bind ass1 (executor-assignment cluster storm-id executor-id1))
      (bind ass2 (executor-assignment cluster storm-id executor-id2))
      (is (not-nil? ass1))
      (is (not-nil? ass2))
      (is (not= active-supervisor (first (executor-assignment cluster storm-id executor-id2))))
      (is (not= active-supervisor (first (executor-assignment cluster storm-id executor-id1))))
      (check-consistency cluster "test")

      (doseq [supervisor-id (.supervisors state nil)]
        (kill-supervisor cluster supervisor-id))

      (advance-cluster-time cluster 90)
      (bind ass1 (executor-assignment cluster storm-id executor-id1))
      (bind ass2 (executor-assignment cluster storm-id executor-id2))
      (is (nil? ass1))
      (is (nil? ass2))
      (check-consistency cluster "test" :assigned? false)

      (add-supervisor cluster)
      (advance-cluster-time cluster 11)
      (check-consistency cluster "test")
      )))


(deftest test-reassignment-to-constrained-cluster
  (with-simulated-time-local-cluster [cluster :supervisors 0
    :daemon-conf {SUPERVISOR-ENABLE false
                  NIMBUS-TASK-LAUNCH-SECS 60
                  NIMBUS-TASK-TIMEOUT-SECS 20
                  NIMBUS-MONITOR-FREQ-SECS 10
                  NIMBUS-SUPERVISOR-TIMEOUT-SECS 100
                  TOPOLOGY-ACKER-EXECUTORS 0
                  TOPOLOGY-EVENTLOGGER-EXECUTORS 0}]
    (letlocals
      (add-supervisor cluster :ports 1 :id "a")
      (add-supervisor cluster :ports 1 :id "b")
      (bind conf (:daemon-conf cluster))
      (bind topology (Thrift/buildTopology
                       {"1" (Thrift/prepareSpoutDetails
                              (TestPlannerSpout. true) (Integer. 2))}
                       {}))
      (bind state (:storm-cluster-state cluster))
      (submit-local-topology (:nimbus cluster) "test" {TOPOLOGY-WORKERS 2} topology)
      (advance-cluster-time cluster 11)
      (check-consistency cluster "test")
      (bind storm-id (StormCommon/getStormId state "test"))
      (bind [executor-id1 executor-id2]  (topology-executors cluster storm-id))
      (bind ass1 (executor-assignment cluster storm-id executor-id1))
      (bind ass2 (executor-assignment cluster storm-id executor-id2))

      (advance-cluster-time cluster 30)
      (do-executor-heartbeat cluster storm-id executor-id1)
      (do-executor-heartbeat cluster storm-id executor-id2)

      (advance-cluster-time cluster 13)
      (is (= ass1 (executor-assignment cluster storm-id executor-id1)))
      (is (= ass2 (executor-assignment cluster storm-id executor-id2)))
      (kill-supervisor cluster "b")
      (do-executor-heartbeat cluster storm-id executor-id1)

      (advance-cluster-time cluster 11)
      (do-executor-heartbeat cluster storm-id executor-id1)

      (advance-cluster-time cluster 11)
      (do-executor-heartbeat cluster storm-id executor-id1)

      (advance-cluster-time cluster 11)
      (do-executor-heartbeat cluster storm-id executor-id1)

      (advance-cluster-time cluster 11)
      (do-executor-heartbeat cluster storm-id executor-id1)

      (check-consistency cluster "test")
      (is (= 1 (storm-num-workers state "test")))
      )))

(defn check-executor-distribution [slot-executors distribution]
  (check-distribution (vals slot-executors) distribution))

(defn check-num-nodes [slot-executors num-nodes]
  (let [nodes (->> slot-executors keys (map first) set)]
    (is (= num-nodes (count nodes)))
    ))

(deftest test-reassign-squeezed-topology
  (with-simulated-time-local-cluster [cluster :supervisors 1 :ports-per-supervisor 1
    :daemon-conf {SUPERVISOR-ENABLE false
                  NIMBUS-TASK-LAUNCH-SECS 60
                  NIMBUS-TASK-TIMEOUT-SECS 20
                  NIMBUS-MONITOR-FREQ-SECS 10
                  TOPOLOGY-ACKER-EXECUTORS 0
                  TOPOLOGY-EVENTLOGGER-EXECUTORS 0}]
    (letlocals
      (bind topology (Thrift/buildTopology
                        {"1" (Thrift/prepareSpoutDetails
                               (TestPlannerSpout. true) (Integer. 9))}
                        {}))
      (bind state (:storm-cluster-state cluster))
      (submit-local-topology (:nimbus cluster) "test" {TOPOLOGY-WORKERS 4} topology)  ; distribution should be 2, 2, 2, 3 ideally
      (advance-cluster-time cluster 11)
      (bind storm-id (StormCommon/getStormId state "test"))
      (bind slot-executors (slot-assignments cluster storm-id))
      (check-executor-distribution slot-executors [9])
      (check-consistency cluster "test")

      (add-supervisor cluster :ports 2)
      (advance-cluster-time cluster 11)
      (bind slot-executors (slot-assignments cluster storm-id))
      (bind executor->start (executor-start-times cluster storm-id))
      (check-executor-distribution slot-executors [3 3 3])
      (check-consistency cluster "test")

      (add-supervisor cluster :ports 8)
      ;; this actually works for any time > 0, since zookeeper fires an event causing immediate reassignment
      ;; doesn't work for time = 0 because it's not waiting for cluster yet, so test might happen before reassignment finishes
      (advance-cluster-time cluster 11)
      (bind slot-executors2 (slot-assignments cluster storm-id))
      (bind executor->start2 (executor-start-times cluster storm-id))
      (check-executor-distribution slot-executors2 [2 2 2 3])
      (check-consistency cluster "test")

      (bind common (first (Utils/findOne (proxy [IPredicate] []
                                           (test [[k v]] (= 3 (count v)))) slot-executors2)))
      (is (not-nil? common))
      (is (= (slot-executors2 common) (slot-executors common)))

      ;; check that start times are changed for everything but the common one
      (bind same-executors (slot-executors2 common))
      (bind changed-executors (apply concat (vals (dissoc slot-executors2 common))))
      (doseq [t same-executors]
        (is (= (executor->start t) (executor->start2 t))))
      (doseq [t changed-executors]
        (is (not= (executor->start t) (executor->start2 t))))
      )))

(deftest test-rebalance
  (with-simulated-time-local-cluster [cluster :supervisors 1 :ports-per-supervisor 3
    :daemon-conf {SUPERVISOR-ENABLE false
                  NIMBUS-MONITOR-FREQ-SECS 10
                  TOPOLOGY-MESSAGE-TIMEOUT-SECS 30
                  TOPOLOGY-ACKER-EXECUTORS 0
                  TOPOLOGY-EVENTLOGGER-EXECUTORS 0}]
    (letlocals
      (bind topology (Thrift/buildTopology
                        {"1" (Thrift/prepareSpoutDetails
                               (TestPlannerSpout. true) (Integer. 3))}
                        {}))
      (bind state (:storm-cluster-state cluster))
      (submit-local-topology (:nimbus cluster)
                             "test"
                             {TOPOLOGY-WORKERS 3
                              TOPOLOGY-MESSAGE-TIMEOUT-SECS 60} topology)
      (advance-cluster-time cluster 11)
      (bind storm-id (StormCommon/getStormId state "test"))
      (add-supervisor cluster :ports 3)
      (add-supervisor cluster :ports 3)

      (advance-cluster-time cluster 11)

      (bind slot-executors (slot-assignments cluster storm-id))
      ;; check that all workers are on one machine
      (check-executor-distribution slot-executors [1 1 1])
      (check-num-nodes slot-executors 1)
      (.rebalance (:nimbus cluster) "test" (RebalanceOptions.))

      (advance-cluster-time cluster 30)
      (check-executor-distribution slot-executors [1 1 1])
      (check-num-nodes slot-executors 1)


      (advance-cluster-time cluster 30)
      (bind slot-executors (slot-assignments cluster storm-id))
      (check-executor-distribution slot-executors [1 1 1])
      (check-num-nodes slot-executors 3)

      (is (thrown? InvalidTopologyException
                   (.rebalance (:nimbus cluster) "test"
                     (doto (RebalanceOptions.)
                       (.set_num_executors {"1" 0})
                       ))))
      )))

;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
(deftest test-rebalance-change-parallelism
  (with-simulated-time-local-cluster [cluster :supervisors 4 :ports-per-supervisor 3
    :daemon-conf {SUPERVISOR-ENABLE false
                  NIMBUS-MONITOR-FREQ-SECS 10
                  TOPOLOGY-ACKER-EXECUTORS 0
                  TOPOLOGY-EVENTLOGGER-EXECUTORS 0}]
    (letlocals
      (bind topology (Thrift/buildTopology
                        {"1" (Thrift/prepareSpoutDetails
                               (TestPlannerSpout. true) (Integer. 6)
                                {TOPOLOGY-TASKS 12})}
                        {}))
      (bind state (:storm-cluster-state cluster))
      (submit-local-topology (:nimbus cluster)
                             "test"
                             {TOPOLOGY-WORKERS 3
                              TOPOLOGY-MESSAGE-TIMEOUT-SECS 30} topology)
      (advance-cluster-time cluster 11)
      (bind storm-id (StormCommon/getStormId state "test"))
      (bind checker (fn [distribution]
                      (check-executor-distribution
                        (slot-assignments cluster storm-id)
                        distribution)))
      (checker [2 2 2])

      (.rebalance (:nimbus cluster) "test"
                  (doto (RebalanceOptions.)
                    (.set_num_workers 6)
                    ))
      (advance-cluster-time cluster 29)
      (checker [2 2 2])
      (advance-cluster-time cluster 3)
      (checker [1 1 1 1 1 1])

      (.rebalance (:nimbus cluster) "test"
                  (doto (RebalanceOptions.)
                    (.set_num_executors {"1" 1})
                    ))
      (advance-cluster-time cluster 29)
      (checker [1 1 1 1 1 1])
      (advance-cluster-time cluster 3)
      (checker [1])

      (.rebalance (:nimbus cluster) "test"
                  (doto (RebalanceOptions.)
                    (.set_num_executors {"1" 8})
                    (.set_num_workers 4)
                    ))
      (advance-cluster-time cluster 32)
      (checker [2 2 2 2])
      (check-consistency cluster "test")

      (bind executor-info (->> (storm-component->executor-info cluster "test")
                               (map-val #(map executor->tasks %))))
      (check-distribution (executor-info "1") [2 2 2 2 1 1 1 1])

      )))


(defn check-for-collisions [state]
 (log-message "Checking for collision")
 (let [assignments (.assignments state nil)]
   (log-message "Assignemts: " assignments)
   (let [id->node->ports (into {} (for [id assignments
                                                :let [executor->node+port (:executor->node+port (clojurify-assignment (.assignmentInfo state id nil)))
                                                      node+ports (set (.values executor->node+port))
                                                      node->ports (apply merge-with (fn [a b] (distinct (concat a b))) (for [[node port] node+ports] {node [port]}))]]
                                                {id node->ports}))
         _ (log-message "id->node->ports: " id->node->ports)
         all-nodes (apply merge-with (fn [a b] 
                                        (let [ret (concat a b)]
                                              (log-message "Can we combine " (pr-str a) " and " (pr-str b) " without collisions? " (apply distinct? ret) " => " (pr-str ret)) 
                                              (is (apply distinct? ret))
                                              (distinct ret)))
                          (.values id->node->ports))]
)))

(deftest test-rebalance-constrained-cluster
  (with-simulated-time-local-cluster [cluster :supervisors 1 :ports-per-supervisor 4
    :daemon-conf {SUPERVISOR-ENABLE false
                  NIMBUS-MONITOR-FREQ-SECS 10
                  TOPOLOGY-MESSAGE-TIMEOUT-SECS 30
                  TOPOLOGY-ACKER-EXECUTORS 0
                  TOPOLOGY-EVENTLOGGER-EXECUTORS 0}]
    (letlocals
      (bind topology (Thrift/buildTopology
                        {"1" (Thrift/prepareSpoutDetails
                               (TestPlannerSpout. true) (Integer. 3))}
                        {}))
      (bind topology2 (Thrift/buildTopology
                        {"1" (Thrift/prepareSpoutDetails
                               (TestPlannerSpout. true) (Integer. 3))}
                        {}))
      (bind topology3 (Thrift/buildTopology
                        {"1" (Thrift/prepareSpoutDetails
                               (TestPlannerSpout. true) (Integer. 3))}
                        {}))
      (bind state (:storm-cluster-state cluster))
      (submit-local-topology (:nimbus cluster)
                             "test"
                             {TOPOLOGY-WORKERS 3
                              TOPOLOGY-MESSAGE-TIMEOUT-SECS 90} topology)
      (submit-local-topology (:nimbus cluster)
                             "test2"
                             {TOPOLOGY-WORKERS 3
                              TOPOLOGY-MESSAGE-TIMEOUT-SECS 90} topology2)
      (submit-local-topology (:nimbus cluster)
                             "test3"
                             {TOPOLOGY-WORKERS 3
                              TOPOLOGY-MESSAGE-TIMEOUT-SECS 90} topology3)

      (advance-cluster-time cluster 11)

      (check-for-collisions state)
      (.rebalance (:nimbus cluster) "test" (doto (RebalanceOptions.)
                    (.set_num_workers 4)
                    (.set_wait_secs 0)
                    ))

      (advance-cluster-time cluster 11)
      (check-for-collisions state)

      (advance-cluster-time cluster 30)
      (check-for-collisions state)
      )))


(deftest test-submit-invalid
  (with-simulated-time-local-cluster [cluster
    :daemon-conf {SUPERVISOR-ENABLE false
                  TOPOLOGY-ACKER-EXECUTORS 0
                  TOPOLOGY-EVENTLOGGER-EXECUTORS 0
                  NIMBUS-EXECUTORS-PER-TOPOLOGY 8
                  NIMBUS-SLOTS-PER-TOPOLOGY 8}]
    (letlocals
      (bind topology (Thrift/buildTopology
                        {"1" (Thrift/prepareSpoutDetails
                               (TestPlannerSpout. true) (Integer. 1)
                               {TOPOLOGY-TASKS 1})}
                        {}))
      (is (thrown? InvalidTopologyException
        (submit-local-topology (:nimbus cluster)
                               "test/aaa"
                               {}
                               topology)))
      (bind topology (Thrift/buildTopology
                      {"1" (Thrift/prepareSpoutDetails
                             (TestPlannerSpout. true) (Integer. 16)
                             {TOPOLOGY-TASKS 16})}
                      {}))
      (bind state (:storm-cluster-state cluster))
      (is (thrown? InvalidTopologyException
                   (submit-local-topology (:nimbus cluster)
                                          "test"
                                          {TOPOLOGY-WORKERS 3}
                                          topology)))
      (bind topology (Thrift/buildTopology
                      {"1" (Thrift/prepareSpoutDetails
                             (TestPlannerSpout. true) (Integer. 5)
                             {TOPOLOGY-TASKS 5})}
                      {}))
      (is (thrown? InvalidTopologyException
                   (submit-local-topology (:nimbus cluster)
                                          "test"
                                          {TOPOLOGY-WORKERS 16}
                                          topology)))
      (is (nil? (submit-local-topology (:nimbus cluster)
                                       "test"
                                       {TOPOLOGY-WORKERS 8}
                                       topology))))))

(defnk mock-leader-elector [:is-leader true :leader-name "test-host" :leader-port 9999]
  (let [leader-address (NimbusInfo. leader-name leader-port true)]
    (reify ILeaderElector
      (prepare [this conf] true)
      (isLeader [this] is-leader)
      (addToLeaderLockQueue [this] true)
      (getLeader [this] leader-address)
      (getAllNimbuses [this] `(leader-address))
      (close [this] true))))

(deftest test-cleans-corrupt
  (with-inprocess-zookeeper zk-port
    (with-local-tmp [nimbus-dir]
      (with-open [_ (MockedZookeeper. (proxy [Zookeeper] []
                      (zkLeaderElectorImpl [conf] (mock-leader-elector))))]
        (letlocals
         (bind conf (merge (clojurify-structure (ConfigUtils/readStormConfig))
                           {STORM-ZOOKEEPER-SERVERS ["localhost"]
                            STORM-CLUSTER-MODE "local"
                            STORM-ZOOKEEPER-PORT zk-port
                            STORM-LOCAL-DIR nimbus-dir}))
         (bind cluster-state (ClusterUtils/mkStormClusterState conf nil (ClusterStateContext.)))
         (bind nimbus (nimbus/service-handler conf (nimbus/standalone-nimbus)))
         (bind topology (Thrift/buildTopology
                         {"1" (Thrift/prepareSpoutDetails
                                (TestPlannerSpout. true) (Integer. 3))}
                         {}))
         (submit-local-topology nimbus "t1" {} topology)
         (submit-local-topology nimbus "t2" {} topology)
         (bind storm-id1 (StormCommon/getStormId cluster-state "t1"))
         (bind storm-id2 (StormCommon/getStormId cluster-state "t2"))
         (.shutdown nimbus)
         (let [blob-store (Utils/getNimbusBlobStore conf nil)]
           (nimbus/blob-rm-topology-keys storm-id1 blob-store cluster-state)
           (.shutdown blob-store))
         (bind nimbus (nimbus/service-handler conf (nimbus/standalone-nimbus)))
         (is ( = #{storm-id2} (set (.activeStorms cluster-state))))
         (.shutdown nimbus)
         (.disconnect cluster-state)
         )))))

;(deftest test-no-overlapping-slots
;  ;; test that same node+port never appears across 2 assignments
;  )

;(deftest test-stateless
;  ;; test that nimbus can die and restart without any problems
;  )

(deftest test-clean-inbox
  "Tests that the inbox correctly cleans jar files."
  (with-simulated-time
    (with-local-tmp [dir-location]
      (let [dir (File. dir-location)
            mk-file (fn [name seconds-ago]
                      (let [f (File. (str dir-location "/" name))
                            t (- (Time/currentTimeMillis) (* seconds-ago 1000))]
                        (FileUtils/touch f)
                        (.setLastModified f t)))
            assert-files-in-dir (fn [compare-file-names]
                                  (let [file-names (map #(.getName %) (file-seq dir))]
                                    (is (= (sort compare-file-names)
                                          (sort (filter #(.endsWith % ".jar") file-names))
                                          ))))]
        ;; Make three files a.jar, b.jar, c.jar.
        ;; a and b are older than c and should be deleted first.
        (advance-time-secs! 100)
        (doseq [fs [["a.jar" 20] ["b.jar" 20] ["c.jar" 0]]]
          (apply mk-file fs))
        (assert-files-in-dir ["a.jar" "b.jar" "c.jar"])
        (nimbus/clean-inbox dir-location 10)
        (assert-files-in-dir ["c.jar"])
        ;; Cleanit again, c.jar should stay
        (advance-time-secs! 5)
        (nimbus/clean-inbox dir-location 10)
        (assert-files-in-dir ["c.jar"])
        ;; Advance time, clean again, c.jar should be deleted.
        (advance-time-secs! 5)
        (nimbus/clean-inbox dir-location 10)
        (assert-files-in-dir [])
        ))))

(deftest test-leadership
  "Tests that leader actions can only be performed by master and non leader fails to perform the same actions."
  (with-inprocess-zookeeper zk-port
    (with-local-tmp [nimbus-dir]
      (with-open [_ (MockedZookeeper. (proxy [Zookeeper] []
                      (zkLeaderElectorImpl [conf] (mock-leader-elector))))]
        (letlocals
          (bind conf (merge (clojurify-structure (ConfigUtils/readStormConfig))
                       {STORM-ZOOKEEPER-SERVERS ["localhost"]
                        STORM-CLUSTER-MODE "local"
                        STORM-ZOOKEEPER-PORT zk-port
                        STORM-LOCAL-DIR nimbus-dir}))
          (bind cluster-state (ClusterUtils/mkStormClusterState conf nil (ClusterStateContext.)))
          (bind nimbus (nimbus/service-handler conf (nimbus/standalone-nimbus)))
          (bind topology (Thrift/buildTopology
                           {"1" (Thrift/prepareSpoutDetails
                                  (TestPlannerSpout. true) (Integer. 3))}
                           {}))

          (with-open [_ (MockedZookeeper. (proxy [Zookeeper] []
                          (zkLeaderElectorImpl [conf] (mock-leader-elector :is-leader false))))]

            (letlocals
              (bind non-leader-cluster-state (ClusterUtils/mkStormClusterState conf nil (ClusterStateContext.)))
              (bind non-leader-nimbus (nimbus/service-handler conf (nimbus/standalone-nimbus)))

              ;first we verify that the master nimbus can perform all actions, even with another nimbus present.
              (submit-local-topology nimbus "t1" {} topology)
              ;; Instead of sleeping until topology is scheduled, rebalance topology so mk-assignments is called.
              (.rebalance nimbus "t1" (doto (RebalanceOptions.) (.set_wait_secs 0)))
              (Thread/sleep 1000)
              (.deactivate nimbus "t1")
              (.activate nimbus "t1")
              (.rebalance nimbus "t1" (RebalanceOptions.))
              (.killTopology nimbus "t1")

              ;now we verify that non master nimbus can not perform any of the actions.
              (is (thrown? RuntimeException
                    (submit-local-topology non-leader-nimbus
                      "failing"
                      {}
                      topology)))

              (is (thrown? RuntimeException
                    (.killTopology non-leader-nimbus
                      "t1")))

              (is (thrown? RuntimeException
                    (.activate non-leader-nimbus "t1")))

              (is (thrown? RuntimeException
                    (.deactivate non-leader-nimbus "t1")))

              (is (thrown? RuntimeException
                    (.rebalance non-leader-nimbus "t1" (RebalanceOptions.))))
              (.shutdown non-leader-nimbus)
              (.disconnect non-leader-cluster-state)
              ))
          (.shutdown nimbus)
          (.disconnect cluster-state))))))

(deftest test-nimbus-iface-submitTopologyWithOpts-checks-authorization
  (with-local-cluster [cluster
                       :daemon-conf {NIMBUS-AUTHORIZER
                          "org.apache.storm.security.auth.authorizer.DenyAuthorizer"}]
    (let [
          nimbus (:nimbus cluster)
          topology (Thrift/buildTopology {} {})
         ]
      (is (thrown? AuthorizationException
          (submit-local-topology-with-opts nimbus "mystorm" {} topology
            (SubmitOptions. TopologyInitialStatus/INACTIVE))
        ))
    )
  )
)

(deftest test-nimbus-iface-methods-check-authorization
  (with-local-cluster [cluster
                       :daemon-conf {NIMBUS-AUTHORIZER
                          "org.apache.storm.security.auth.authorizer.DenyAuthorizer"}]
    (let [
          nimbus (:nimbus cluster)
          topology (Thrift/buildTopology {} {})
         ]
      ; Fake good authorization as part of setup.
      (mocking [nimbus/check-authorization!]
          (submit-local-topology-with-opts nimbus "test" {} topology
              (SubmitOptions. TopologyInitialStatus/INACTIVE))
      )
      (stubbing [nimbus/storm-active? true]
        (is (thrown? AuthorizationException
          (.rebalance nimbus "test" (RebalanceOptions.))
          ))
      )
      (is (thrown? AuthorizationException
        (.activate nimbus "test")
        ))
      (is (thrown? AuthorizationException
        (.deactivate nimbus "test")
        ))
    )
  )
)

(deftest test-nimbus-check-authorization-params
  (with-local-cluster [cluster
                       :daemon-conf {NIMBUS-AUTHORIZER "org.apache.storm.security.auth.authorizer.NoopAuthorizer"}]
    (let [nimbus (:nimbus cluster)
          topology-name "test-nimbus-check-autho-params"
          topology (Thrift/buildTopology {} {})]

      (submit-local-topology-with-opts nimbus topology-name {} topology
          (SubmitOptions. TopologyInitialStatus/INACTIVE))

      (let [expected-name topology-name
            expected-conf {TOPOLOGY-NAME expected-name
                           "foo" "bar"}]

        (testing "getTopologyConf calls check-authorization! with the correct parameters."
          (let [expected-operation "getTopologyConf"
                expected-conf-json (JSONValue/toJSONString expected-conf)]
            (stubbing [nimbus/check-authorization! nil
                       nimbus/try-read-storm-conf expected-conf]
              (try
                (is (= expected-conf
                       (->> (.getTopologyConf nimbus "fake-id")
                            JSONValue/parse 
                            clojurify-structure)))
                (catch NotAliveException e)
                (finally
                  (verify-first-call-args-for-indices
                    nimbus/check-authorization!
                      [1 2 3] expected-name expected-conf expected-operation))))))

        (testing "getTopology calls check-authorization! with the correct parameters."
          (let [expected-operation "getTopology"
                common-spy (->>
                             (proxy [StormCommon] []
                                    (systemTopologyImpl [conf topology] nil))
                           Mockito/spy)]
            (with-open [- (StormCommonInstaller. common-spy)]
              (stubbing [nimbus/check-authorization! nil
                       nimbus/try-read-storm-conf expected-conf
                       nimbus/try-read-storm-topology nil]
                (try
                  (.getTopology nimbus "fake-id")
                  (catch NotAliveException e)
                  (finally
                    (verify-first-call-args-for-indices
                      nimbus/check-authorization!
                        [1 2 3] expected-name expected-conf expected-operation)
                    (. (Mockito/verify common-spy)
                      (systemTopologyImpl (Matchers/eq expected-conf)
                                          (Matchers/any)))))))))

        (testing "getUserTopology calls check-authorization with the correct parameters."
          (let [expected-operation "getUserTopology"]
            (stubbing [nimbus/check-authorization! nil
                       nimbus/try-read-storm-conf expected-conf
                       nimbus/try-read-storm-topology nil]
              (try
                (.getUserTopology nimbus "fake-id")
                (catch NotAliveException e)
                (finally
                  (verify-first-call-args-for-indices
                    nimbus/check-authorization!
                      [1 2 3] expected-name expected-conf expected-operation)
                  (verify-first-call-args-for-indices
                    nimbus/try-read-storm-topology [0] "fake-id"))))))))))

(deftest test-nimbus-iface-getTopology-methods-throw-correctly
  (with-local-cluster [cluster]
    (let [
          nimbus (:nimbus cluster)
          id "bogus ID"
         ]
      (is (thrown? NotAliveException (.getTopology nimbus id)))
      (try
        (.getTopology nimbus id)
        (catch NotAliveException e
           (is (= id (.get_msg e)))
        )
      )

      (is (thrown? NotAliveException (.getTopologyConf nimbus id)))
      (try (.getTopologyConf nimbus id)
        (catch NotAliveException e
           (is (= id (.get_msg e)))
        )
      )

      (is (thrown? NotAliveException (.getTopologyInfo nimbus id)))
      (try (.getTopologyInfo nimbus id)
        (catch NotAliveException e
           (is (= id (.get_msg e)))
        )
      )

      (is (thrown? NotAliveException (.getUserTopology nimbus id)))
      (try (.getUserTopology nimbus id)
        (catch NotAliveException e
           (is (= id (.get_msg e)))
        )
      )
    )
  )
)

(deftest test-nimbus-iface-getClusterInfo-filters-topos-without-bases
  (with-local-cluster [cluster]
    (let [
          nimbus (:nimbus cluster)
          bogus-secs 42
          bogus-type "bogusType"
          bogus-bases {
                 "1" nil
                 "2" {:launch-time-secs bogus-secs
                        :storm-name "id2-name"
                        :status {:type bogus-type}}
                 "3" nil
                 "4" {:launch-time-secs bogus-secs
                        :storm-name "id4-name"
                        :status {:type bogus-type}}
                }
        ]
      (stubbing [nimbus/nimbus-topology-bases bogus-bases
                 nimbus/get-blob-replication-count 1]
        (let [topos (.get_topologies (.getClusterInfo nimbus))]
          ; The number of topologies in the summary is correct.
          (is (= (count
            (filter (fn [b] (second b)) bogus-bases)) (count topos)))
          ; Each topology present has a valid name.
          (is (empty?
            (filter (fn [t] (or (nil? t) (nil? (.get_name t)))) topos)))
          ; The topologies are those with valid bases.
          (is (empty?
            (filter (fn [t]
              (or
                (nil? t)
                (not (number? (read-string (.get_id t))))
                (odd? (read-string (.get_id t)))
              )) topos)))
        )
      )
    )
  )
)

(deftest test-defserverfn-numbus-iface-instance
  (test-nimbus-iface-submitTopologyWithOpts-checks-authorization)
  (test-nimbus-iface-methods-check-authorization)
  (test-nimbus-iface-getTopology-methods-throw-correctly)
  (test-nimbus-iface-getClusterInfo-filters-topos-without-bases)
)

(deftest test-nimbus-data-acls
  (testing "nimbus-data uses correct ACLs"
    (let [scheme "digest"
          digest "storm:thisisapoorpassword"
          auth-conf (merge (clojurify-structure (ConfigUtils/readStormConfig))
                    {STORM-ZOOKEEPER-AUTH-SCHEME scheme
                     STORM-ZOOKEEPER-AUTH-PAYLOAD digest
                     STORM-PRINCIPAL-TO-LOCAL-PLUGIN "org.apache.storm.security.auth.DefaultPrincipalToLocal"
                     NIMBUS-THRIFT-PORT 6666})
          expected-acls nimbus/NIMBUS-ZK-ACLS
          fake-inimbus (reify INimbus (getForcedScheduler [this] nil))
          fake-cu (proxy [ConfigUtils] []
                    (nimbusTopoHistoryStateImpl [conf] nil))
          fake-utils (proxy [Utils] []
                       (newInstanceImpl [_])
                       (makeUptimeComputer [] (proxy [Utils$UptimeComputer] []
                                                (upTime [] 0))))
          cluster-utils (Mockito/mock ClusterUtils)
	  fake-common (proxy [StormCommon] []
                             (mkAuthorizationHandler [_] nil))]
      (with-open [_ (ConfigUtilsInstaller. fake-cu)
                  _ (UtilsInstaller. fake-utils)
                  - (StormCommonInstaller. fake-common)
                  zk-le (MockedZookeeper. (proxy [Zookeeper] []
                          (zkLeaderElectorImpl [conf] nil)))
                  mocked-cluster (MockedCluster. cluster-utils)]
        (stubbing [nimbus/file-cache-map nil
                 nimbus/mk-blob-cache-map nil
                 nimbus/mk-bloblist-cache-map nil
                 nimbus/mk-scheduler nil]
          (nimbus/nimbus-data auth-conf fake-inimbus)
          (.mkStormClusterStateImpl (Mockito/verify cluster-utils (Mockito/times 1)) (Mockito/any) (Mockito/eq expected-acls) (Mockito/any))
          )))))

(deftest test-file-bogus-download
  (with-local-cluster [cluster :daemon-conf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0 TOPOLOGY-EVENTLOGGER-EXECUTORS 0}]
    (let [nimbus (:nimbus cluster)]
      (is (thrown-cause? AuthorizationException (.beginFileDownload nimbus nil)))
      (is (thrown-cause? AuthorizationException (.beginFileDownload nimbus "")))
      (is (thrown-cause? AuthorizationException (.beginFileDownload nimbus "/bogus-path/foo")))
      )))

(deftest test-validate-topo-config-on-submit
  (with-local-cluster [cluster]
    (let [nimbus (:nimbus cluster)
          topology (Thrift/buildTopology {} {})
          bad-config {"topology.isolate.machines" "2"}]
      ; Fake good authorization as part of setup.
      (mocking [nimbus/check-authorization!]
        (is (thrown-cause? InvalidTopologyException
          (submit-local-topology-with-opts nimbus "test" bad-config topology
                                           (SubmitOptions.))))))))

(deftest test-stateless-with-scheduled-topology-to-be-killed
  ; tests regression of STORM-856
  (with-inprocess-zookeeper zk-port
    (with-local-tmp [nimbus-dir]
      (letlocals
        (bind conf (merge (clojurify-structure (ConfigUtils/readStormConfig))
                     {STORM-ZOOKEEPER-SERVERS ["localhost"]
                      STORM-CLUSTER-MODE "local"
                      STORM-ZOOKEEPER-PORT zk-port
                      STORM-LOCAL-DIR nimbus-dir}))
        (bind cluster-state (ClusterUtils/mkStormClusterState conf nil (ClusterStateContext.)))
        (bind nimbus (nimbus/service-handler conf (nimbus/standalone-nimbus)))
        (Time/sleepSecs 1)
        (bind topology (Thrift/buildTopology
                         {"1" (Thrift/prepareSpoutDetails
                                (TestPlannerSpout. true) (Integer. 3))}
                         {}))
        (submit-local-topology nimbus "t1" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 30} topology)
        ; make transition for topology t1 to be killed -> nimbus applies this event to cluster state
        (.killTopology nimbus "t1")
        ; shutdown nimbus immediately to achieve nimbus doesn't handle event right now
        (.shutdown nimbus)

        ; in startup of nimbus it reads cluster state and take proper actions
        ; in this case nimbus registers topology transition event to scheduler again
        ; before applying STORM-856 nimbus was killed with NPE
        (bind nimbus (nimbus/service-handler conf (nimbus/standalone-nimbus)))
        (.shutdown nimbus)
        (.disconnect cluster-state)
        ))))

(deftest test-topology-action-notifier
  (with-inprocess-zookeeper zk-port
    (with-local-tmp [nimbus-dir]
      (with-open [_ (MockedZookeeper. (proxy [Zookeeper] []
                      (zkLeaderElectorImpl [conf] (mock-leader-elector))))]
        (letlocals
          (bind conf (merge (clojurify-structure (ConfigUtils/readStormConfig))
                       {STORM-ZOOKEEPER-SERVERS ["localhost"]
                        STORM-CLUSTER-MODE "local"
                        STORM-ZOOKEEPER-PORT zk-port
                        STORM-LOCAL-DIR nimbus-dir
                        NIMBUS-TOPOLOGY-ACTION-NOTIFIER-PLUGIN (.getName InMemoryTopologyActionNotifier)}))
          (bind cluster-state (ClusterUtils/mkStormClusterState conf nil (ClusterStateContext.)))
          (bind nimbus (nimbus/service-handler conf (nimbus/standalone-nimbus)))
          (bind notifier (InMemoryTopologyActionNotifier.))
          (Time/sleepSecs 1)
          (bind topology (Thrift/buildTopology
                           {"1" (Thrift/prepareSpoutDetails
                                  (TestPlannerSpout. true) (Integer. 3))}
                           {}))
          (submit-local-topology nimbus "test-notification" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 30} topology)

          (.deactivate nimbus "test-notification")

          (.activate nimbus "test-notification")

          (.rebalance nimbus "test-notification" (doto (RebalanceOptions.)
                                                   (.set_wait_secs 0)))

          (.killTopologyWithOpts nimbus "test-notification" (doto (KillOptions.)
                                                      (.set_wait_secs 0)))

          (.shutdown nimbus)

          ; ensure notifier was invoked for each action,and in the correct order.
          (is (= ["submitTopology", "activate", "deactivate", "activate", "rebalance", "killTopology"]
                (.getTopologyActions notifier "test-notification")))
          (.disconnect cluster-state)
          )))))

(deftest test-debug-on-component
  (with-local-cluster [cluster]
    (let [nimbus (:nimbus cluster)
          topology (Thrift/buildTopology
                     {"spout" (Thrift/prepareSpoutDetails
                                (TestPlannerSpout. true) (Integer. 3))}
                     {})]
        (submit-local-topology nimbus "t1" {TOPOLOGY-WORKERS 1} topology)
        (.debug nimbus "t1" "spout" true 100))))

(deftest test-debug-on-global
  (with-local-cluster [cluster]
    (let [nimbus (:nimbus cluster)
          topology (Thrift/buildTopology
                     {"spout" (Thrift/prepareSpoutDetails
                                (TestPlannerSpout. true) (Integer. 3))}
                     {})]
      (submit-local-topology nimbus "t1" {TOPOLOGY-WORKERS 1} topology)
      (.debug nimbus "t1" "" true 100))))

;; if the user sends an empty log config, nimbus will say that all 
;; log configs it contains are LogLevelAction/UNCHANGED
(deftest empty-save-config-results-in-all-unchanged-actions
  (with-local-cluster [cluster]
    (let [nimbus (:nimbus cluster)
          previous-config (LogConfig.)
          level (LogLevel.)
          mock-config (LogConfig.)]
      ;; send something with content to nimbus beforehand
      (.set_target_log_level level "ERROR")
      (.set_action level LogLevelAction/UPDATE)
      (.put_to_named_logger_level previous-config "test" level)
      (stubbing [nimbus/check-storm-active! nil
                 nimbus/try-read-storm-conf {}]
        (.setLogConfig nimbus "foo" previous-config)
        (.setLogConfig nimbus "foo" mock-config)
        (let [saved-config (.getLogConfig nimbus "foo")
              levels (.get_named_logger_level saved-config)]
           (is (= (.get_action (.get levels "test")) LogLevelAction/UNCHANGED)))))))

(deftest log-level-update-merges-and-flags-existent-log-level
  (with-local-cluster [cluster]
    (stubbing [nimbus/check-storm-active! nil
               nimbus/try-read-storm-conf {}]
      (let [nimbus (:nimbus cluster)
            previous-config (LogConfig.)
            level (LogLevel.)
            other-level (LogLevel.)
            mock-config (LogConfig.)]
        ;; send something with content to nimbus beforehand
        (.set_target_log_level level "ERROR")
        (.set_action level LogLevelAction/UPDATE)
        (.put_to_named_logger_level previous-config "test" level)

        (.set_target_log_level other-level "DEBUG")
        (.set_action other-level LogLevelAction/UPDATE)
        (.put_to_named_logger_level previous-config "other-test" other-level)
        (.setLogConfig nimbus "foo" previous-config)

        ;; only change "test"
        (.set_target_log_level level "INFO")
        (.set_action level LogLevelAction/UPDATE)
        (.put_to_named_logger_level mock-config "test" level)
        (.setLogConfig nimbus "foo" mock-config)

        (let [saved-config (.getLogConfig nimbus "foo")
              levels (.get_named_logger_level saved-config)]
           (is (= (.get_action (.get levels "test")) LogLevelAction/UPDATE))
           (is (= (.get_target_log_level (.get levels "test")) "INFO"))

           (is (= (.get_action (.get levels "other-test")) LogLevelAction/UNCHANGED))
           (is (= (.get_target_log_level (.get levels "other-test")) "DEBUG")))))))

(defn teardown-heartbeats [id])
(defn teardown-topo-errors [id])
(defn teardown-backpressure-dirs [id])

(defn mock-cluster-state 
  ([] 
    (mock-cluster-state nil nil))
  ([active-topos inactive-topos]
    (mock-cluster-state active-topos inactive-topos inactive-topos inactive-topos)) 
  ([active-topos hb-topos error-topos bp-topos]
    (reify IStormClusterState
      (teardownHeartbeats [this id] (teardown-heartbeats id))
      (teardownTopologyErrors [this id] (teardown-topo-errors id))
      (removeBackpressure [this id] (teardown-backpressure-dirs id))
      (activeStorms [this] active-topos)
      (heartbeatStorms [this] hb-topos)
      (errorTopologies [this] error-topos)
      (backpressureTopologies [this] bp-topos))))

(deftest cleanup-storm-ids-returns-inactive-topos
  (let [mock-state (mock-cluster-state (list "topo1") (list "topo1" "topo2" "topo3"))]
    (stubbing [nimbus/is-leader true
               nimbus/code-ids {}] 
    (is (= (nimbus/cleanup-storm-ids mock-state nil) #{"topo2" "topo3"})))))

(deftest cleanup-storm-ids-performs-union-of-storm-ids-with-active-znodes
  (let [active-topos (list "hb1" "e2" "bp3")
        hb-topos (list "hb1" "hb2" "hb3")
        error-topos (list "e1" "e2" "e3")
        bp-topos (list "bp1" "bp2" "bp3")
        mock-state (mock-cluster-state active-topos hb-topos error-topos bp-topos)]
    (stubbing [nimbus/is-leader true
               nimbus/code-ids {}] 
    (is (= (nimbus/cleanup-storm-ids mock-state nil) 
           #{"hb2" "hb3" "e1" "e3" "bp1" "bp2"})))))

(deftest cleanup-storm-ids-returns-empty-set-when-all-topos-are-active
  (let [active-topos (list "hb1" "hb2" "hb3" "e1" "e2" "e3" "bp1" "bp2" "bp3")
        hb-topos (list "hb1" "hb2" "hb3")
        error-topos (list "e1" "e2" "e3")
        bp-topos (list "bp1" "bp2" "bp3")
        mock-state (mock-cluster-state active-topos hb-topos error-topos bp-topos)]
    (stubbing [nimbus/is-leader true
               nimbus/code-ids {}] 
    (is (= (nimbus/cleanup-storm-ids mock-state nil) 
           #{})))))

(deftest do-cleanup-removes-inactive-znodes
  (let [inactive-topos (list "topo2" "topo3")
        hb-cache (atom (into {}(map vector inactive-topos '(nil nil))))
        mock-state (mock-cluster-state)
        mock-blob-store {}
        conf {}
        nimbus {:conf conf
                :submit-lock mock-blob-store 
                :blob-store {}
                :storm-cluster-state mock-state
                :heartbeats-cache hb-cache}]

    (stubbing [nimbus/is-leader true
               nimbus/blob-rm-topology-keys nil
               nimbus/cleanup-storm-ids inactive-topos]
      (mocking
        [teardown-heartbeats 
         teardown-topo-errors 
         teardown-backpressure-dirs
         nimbus/force-delete-topo-dist-dir
         nimbus/blob-rm-topology-keys] 

        (nimbus/do-cleanup nimbus)

        ;; removed heartbeats znode
        (verify-nth-call-args-for 1 teardown-heartbeats "topo2")
        (verify-nth-call-args-for 2 teardown-heartbeats "topo3")

        ;; removed topo errors znode
        (verify-nth-call-args-for 1 teardown-topo-errors "topo2")
        (verify-nth-call-args-for 2 teardown-topo-errors "topo3")

        ;; removed backpressure znodes
        (verify-nth-call-args-for 1 teardown-backpressure-dirs "topo2")
        (verify-nth-call-args-for 2 teardown-backpressure-dirs "topo3")

        ;; removed topo directories
        (verify-nth-call-args-for 1 nimbus/force-delete-topo-dist-dir conf "topo2")
        (verify-nth-call-args-for 2 nimbus/force-delete-topo-dist-dir conf "topo3")

        ;; removed blob store topo keys
        (verify-nth-call-args-for 1 nimbus/blob-rm-topology-keys "topo2" mock-blob-store mock-state)
        (verify-nth-call-args-for 2 nimbus/blob-rm-topology-keys "topo3" mock-blob-store mock-state)

        ;; remove topos from heartbeat cache
        (is (= (count @hb-cache) 0))))))

(deftest do-cleanup-does-not-teardown-active-topos
  (let [inactive-topos ()
        hb-cache (atom {"topo1" nil "topo2" nil})
        mock-state (mock-cluster-state)
        mock-blob-store {}
        conf {}
        nimbus {:conf conf
                :submit-lock mock-blob-store 
                :blob-store {}
                :storm-cluster-state mock-state
                :heartbeats-cache hb-cache}]

    (stubbing [nimbus/is-leader true
               nimbus/blob-rm-topology-keys nil
               nimbus/cleanup-storm-ids inactive-topos]
      (mocking
        [teardown-heartbeats 
         teardown-topo-errors 
         teardown-backpressure-dirs
         nimbus/force-delete-topo-dist-dir
         nimbus/blob-rm-topology-keys] 

        (nimbus/do-cleanup nimbus)

        (verify-call-times-for teardown-heartbeats 0)
        (verify-call-times-for teardown-topo-errors 0)
        (verify-call-times-for teardown-backpressure-dirs 0)
        (verify-call-times-for nimbus/force-delete-topo-dist-dir 0)
        (verify-call-times-for nimbus/blob-rm-topology-keys 0)

        ;; hb-cache goes down to 1 because only one topo was inactive
        (is (= (count @hb-cache) 2))
        (is (contains? @hb-cache "topo1"))
        (is (contains? @hb-cache "topo2"))))))
