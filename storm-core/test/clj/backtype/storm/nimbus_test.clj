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
(ns backtype.storm.nimbus-test
  (:use [clojure test])
  (:require [backtype.storm.daemon [nimbus :as nimbus]])
  
  (:import [backtype.storm.testing TestWordCounter TestWordSpout TestGlobalCount TestAggregatesCounter])
  (:import [backtype.storm.scheduler INimbus])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  )

(bootstrap)

(defn storm-component->task-info [cluster storm-name]
  (let [storm-id (get-storm-id (:storm-cluster-state cluster) storm-name)
        nimbus (:nimbus cluster)]
    (-> (.getUserTopology nimbus storm-id)
        (storm-task-info (from-json (.getTopologyConf nimbus storm-id)))
        reverse-map)))

(defn storm-component->executor-info [cluster storm-name]
  (let [storm-id (get-storm-id (:storm-cluster-state cluster) storm-name)
        nimbus (:nimbus cluster)
        storm-conf (from-json (.getTopologyConf nimbus storm-id))
        topology (.getUserTopology nimbus storm-id)
        task->component (storm-task-info topology storm-conf)
        state (:storm-cluster-state cluster)
        get-component (comp task->component first)]
    (->> (.assignment-info state storm-id nil)
         :executor->node+port
         keys
         (map (fn [e] {e (get-component e)}))
         (apply merge)
         reverse-map)))

(defn storm-num-workers [state storm-name]
  (let [storm-id (get-storm-id state storm-name)
        assignment (.assignment-info state storm-id nil)]
    (count (reverse-map (:executor->node+port assignment)))
    ))

(defn topology-nodes [state storm-name]
  (let [storm-id (get-storm-id state storm-name)
        assignment (.assignment-info state storm-id nil)]
    (->> assignment
         :executor->node+port
         vals
         (map first)
         set         
         )))

(defn topology-slots [state storm-name]
  (let [storm-id (get-storm-id state storm-name)
        assignment (.assignment-info state storm-id nil)]
    (->> assignment
         :executor->node+port
         vals
         set         
         )))

(defn topology-node-distribution [state storm-name]
  (let [storm-id (get-storm-id state storm-name)
        assignment (.assignment-info state storm-id nil)]
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
        assignment (.assignment-info state storm-id nil)]
    ((:executor->node+port assignment) executor-id)
    ))

(defn executor-start-times [cluster storm-id]
  (let [state (:storm-cluster-state cluster)
        assignment (.assignment-info state storm-id nil)]
    (:executor->start-time-secs assignment)))

(defn do-executor-heartbeat [cluster storm-id executor]
  (let [state (:storm-cluster-state cluster)
        executor->node+port (:executor->node+port (.assignment-info state storm-id nil))
        [node port] (get executor->node+port executor)
        curr-beat (.get-worker-heartbeat state storm-id node port)
        stats (:executor-stats curr-beat)]
    (.worker-heartbeat! state storm-id node port
      {:storm-id storm-id :time-secs (current-time-secs) :uptime 10 :executor-stats (merge stats {executor nil})}
      )))

(defn slot-assignments [cluster storm-id]
  (let [state (:storm-cluster-state cluster)
        assignment (.assignment-info state storm-id nil)]
    (reverse-map (:executor->node+port assignment))
    ))

(defn task-ids [cluster storm-id]
  (let [nimbus (:nimbus cluster)]
    (-> (.getUserTopology nimbus storm-id)
        (storm-task-info (from-json (.getTopologyConf nimbus storm-id)))
        keys)))

(defn topology-executors [cluster storm-id]
  (let [state (:storm-cluster-state cluster)
        assignment (.assignment-info state storm-id nil)]
    (keys (:executor->node+port assignment))
    ))

(defn check-distribution [items distribution]
  (let [dist (->> items (map count) multi-set)]
    (is (= dist (multi-set distribution)))
    ))

(defn disjoint? [& sets]
  (let [combined (apply concat sets)]
    (= (count combined) (count (set combined)))
    ))

(defnk check-consistency [cluster storm-name :assigned? true]
  (let [state (:storm-cluster-state cluster)
        storm-id (get-storm-id state storm-name)
        task-ids (task-ids cluster storm-id)
        assignment (.assignment-info state storm-id nil)
        executor->node+port (:executor->node+port assignment)
        task->node+port (to-task->node+port executor->node+port)
        assigned-task-ids (mapcat executor-id->tasks (keys executor->node+port))
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
  (with-local-cluster [cluster :supervisors 4 :ports-per-supervisor 3 :daemon-conf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0}]
    (let [state (:storm-cluster-state cluster)
          nimbus (:nimbus cluster)]
       (is (thrown? NotAliveException (.getTopologyConf nimbus "bogus-id")))
       (is (thrown? NotAliveException (.getTopology nimbus "bogus-id")))
       (is (thrown? NotAliveException (.getUserTopology nimbus "bogus-id")))
       (is (thrown? NotAliveException (.getTopologyInfo nimbus "bogus-id")))
      )))

(deftest test-assignment
  (with-local-cluster [cluster :supervisors 4 :ports-per-supervisor 3 :daemon-conf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0}]
    (let [state (:storm-cluster-state cluster)
          nimbus (:nimbus cluster)
          topology (thrift/mk-topology
                    {"1" (thrift/mk-spout-spec (TestPlannerSpout. false) :parallelism-hint 3)}
                    {"2" (thrift/mk-bolt-spec {"1" :none} (TestPlannerBolt.) :parallelism-hint 4)
                     "3" (thrift/mk-bolt-spec {"2" :none} (TestPlannerBolt.))})
          topology2 (thrift/mk-topology
                     {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 12)}
                     {"2" (thrift/mk-bolt-spec {"1" :none} (TestPlannerBolt.) :parallelism-hint 6)
                      "3" (thrift/mk-bolt-spec {"1" :global} (TestPlannerBolt.) :parallelism-hint 8)
                      "4" (thrift/mk-bolt-spec {"1" :global "2" :none} (TestPlannerBolt.) :parallelism-hint 4)}
                     )
          _ (submit-local-topology nimbus "mystorm" {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 4} topology)
          task-info (storm-component->task-info cluster "mystorm")]
      (check-consistency cluster "mystorm")
      ;; 3 should be assigned once (if it were optimized, we'd have
      ;; different topology)
      (is (= 1 (count (.assignments state nil))))
      (is (= 1 (count (task-info "1"))))
      (is (= 4 (count (task-info "2"))))
      (is (= 1 (count (task-info "3"))))
      (is (= 4 (storm-num-workers state "mystorm")))
      (submit-local-topology nimbus "storm2" {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 20} topology2)
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

(deftest test-isolated-assignment
  (with-simulated-time-local-cluster [cluster :supervisors 6
                               :ports-per-supervisor 3
                               :inimbus (isolation-nimbus)
                               :daemon-conf {SUPERVISOR-ENABLE false
                                             TOPOLOGY-ACKER-EXECUTORS 0
                                             STORM-SCHEDULER "backtype.storm.scheduler.IsolationScheduler"
                                             ISOLATION-SCHEDULER-MACHINES {"tester1" 3 "tester2" 2}
                                             NIMBUS-MONITOR-FREQ-SECS 10
                                             }]
    (letlocals
      (bind state (:storm-cluster-state cluster))
      (bind nimbus (:nimbus cluster))
      (bind topology (thrift/mk-topology
                      {"1" (thrift/mk-spout-spec (TestPlannerSpout. false) :parallelism-hint 3)}
                      {"2" (thrift/mk-bolt-spec {"1" :none} (TestPlannerBolt.) :parallelism-hint 5)
                       "3" (thrift/mk-bolt-spec {"2" :none} (TestPlannerBolt.))}))
          
      (submit-local-topology nimbus "noniso" {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 4} topology)
      (advance-cluster-time cluster 1)
      (is (= 4 (topology-num-nodes state "noniso")))
      (is (= 4 (storm-num-workers state "noniso")))

      (submit-local-topology nimbus "tester1" {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 6} topology)
      (submit-local-topology nimbus "tester2" {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 6} topology)
      (advance-cluster-time cluster 1)
    
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
  (with-local-cluster [cluster :daemon-conf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0}]
    (let [state (:storm-cluster-state cluster)
          nimbus (:nimbus cluster)
          topology (thrift/mk-topology
                    {"1" (thrift/mk-spout-spec (TestPlannerSpout. false) :parallelism-hint 3 :conf {TOPOLOGY-TASKS 0})}
                    {"2" (thrift/mk-bolt-spec {"1" :none} (TestPlannerBolt.) :parallelism-hint 1 :conf {TOPOLOGY-TASKS 2})
                     "3" (thrift/mk-bolt-spec {"2" :none} (TestPlannerBolt.) :conf {TOPOLOGY-TASKS 5})})
          _ (submit-local-topology nimbus "mystorm" {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 4} topology)
          task-info (storm-component->task-info cluster "mystorm")]
      (check-consistency cluster "mystorm")
      (is (= 0 (count (task-info "1"))))
      (is (= 2 (count (task-info "2"))))
      (is (= 5 (count (task-info "3"))))
      (is (= 2 (storm-num-workers state "mystorm"))) ;; because only 2 executors
      )))

(deftest test-executor-assignments
  (with-local-cluster [cluster :daemon-conf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0}]
    (let [nimbus (:nimbus cluster)
          topology (thrift/mk-topology
                    {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 3 :conf {TOPOLOGY-TASKS 5})}
                    {"2" (thrift/mk-bolt-spec {"1" :none} (TestPlannerBolt.) :parallelism-hint 8 :conf {TOPOLOGY-TASKS 2})
                     "3" (thrift/mk-bolt-spec {"2" :none} (TestPlannerBolt.) :parallelism-hint 3)})
          _ (submit-local-topology nimbus "mystorm" {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 4} topology)
          task-info (storm-component->task-info cluster "mystorm")
          executor-info (->> (storm-component->executor-info cluster "mystorm")
                             (map-val #(map executor-id->tasks %)))]
      (check-consistency cluster "mystorm")
      (is (= 5 (count (task-info "1"))))
      (check-distribution (executor-info "1") [2 2 1])
      
      (is (= 2 (count (task-info "2"))))
      (check-distribution (executor-info "2") [1 1])

      (is (= 3 (count (task-info "3"))))
      (check-distribution (executor-info "3") [1 1 1])
      )))

(deftest test-over-parallelism-assignment
  (with-local-cluster [cluster :supervisors 2 :ports-per-supervisor 5 :daemon-conf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0}]
    (let [state (:storm-cluster-state cluster)
          nimbus (:nimbus cluster)
          topology (thrift/mk-topology
                     {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 21)}
                     {"2" (thrift/mk-bolt-spec {"1" :none} (TestPlannerBolt.) :parallelism-hint 9)
                      "3" (thrift/mk-bolt-spec {"1" :none} (TestPlannerBolt.) :parallelism-hint 2)
                      "4" (thrift/mk-bolt-spec {"1" :none} (TestPlannerBolt.) :parallelism-hint 10)}
                     )
          _ (submit-local-topology nimbus "test" {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 7} topology)
          task-info (storm-component->task-info cluster "test")]
      (check-consistency cluster "test")
      (is (= 21 (count (task-info "1"))))
      (is (= 9 (count (task-info "2"))))
      (is (= 2 (count (task-info "3"))))
      (is (= 10 (count (task-info "4"))))
      (is (= 7 (storm-num-workers state "test")))
    )))



(deftest test-kill-storm
  (with-simulated-time-local-cluster [cluster :supervisors 2 :ports-per-supervisor 5
    :daemon-conf {SUPERVISOR-ENABLE false
                  NIMBUS-TASK-TIMEOUT-SECS 30
                  NIMBUS-MONITOR-FREQ-SECS 10
                  TOPOLOGY-ACKER-EXECUTORS 0}]
    (letlocals
      (bind conf (:daemon-conf cluster))
      (bind topology (thrift/mk-topology
                       {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 14)}
                       {}
                       ))
      (bind state (:storm-cluster-state cluster))
      (submit-local-topology (:nimbus cluster) "test" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 20} topology)
      (bind storm-id (get-storm-id state "test"))
      (advance-cluster-time cluster 5)
      (is (not-nil? (.storm-base state storm-id nil)))
      (is (not-nil? (.assignment-info state storm-id nil)))
      (.killTopology (:nimbus cluster) "test")
      ;; check that storm is deactivated but alive
      (is (= :killed (-> (.storm-base state storm-id nil) :status :type)))
      (is (not-nil? (.assignment-info state storm-id nil)))
      (advance-cluster-time cluster 18)
      ;; check that storm is deactivated but alive
      (is (= 1 (count (.heartbeat-storms state))))
      (advance-cluster-time cluster 3)
      (is (nil? (.storm-base state storm-id nil)))
      (is (nil? (.assignment-info state storm-id nil)))

      ;; cleanup happens on monitoring thread
      (advance-cluster-time cluster 11)
      (is (empty? (.heartbeat-storms state)))
      ;; TODO: check that code on nimbus was cleaned up locally...

      (is (thrown? NotAliveException (.killTopology (:nimbus cluster) "lalala")))
      (submit-local-topology (:nimbus cluster) "2test" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 10} topology)
      (is (thrown? AlreadyAliveException (submit-local-topology (:nimbus cluster) "2test" {} topology)))
      (bind storm-id (get-storm-id state "2test"))
      (is (not-nil? (.storm-base state storm-id nil)))
      (.killTopology (:nimbus cluster) "2test")
      (is (thrown? AlreadyAliveException (submit-local-topology (:nimbus cluster) "2test" {} topology)))
      (advance-cluster-time cluster 5)
      (is (= 1 (count (.heartbeat-storms state))))
      
      (advance-cluster-time cluster 6)
      (is (nil? (.storm-base state storm-id nil)))
      (is (nil? (.assignment-info state storm-id nil)))
      (advance-cluster-time cluster 11)
      (is (= 0 (count (.heartbeat-storms state))))
      
      (submit-local-topology (:nimbus cluster) "test3" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 5} topology)
      (bind storm-id3 (get-storm-id state "test3"))
      (advance-cluster-time cluster 1)
      (.remove-storm! state storm-id3)
      (is (nil? (.storm-base state storm-id3 nil)))
      (is (nil? (.assignment-info state storm-id3 nil)))

      (advance-cluster-time cluster 11)
      (is (= 0 (count (.heartbeat-storms state))))

      ;; this guarantees that monitor thread won't trigger for 10 more seconds
      (advance-time-secs! 11)
      (wait-until-cluster-waiting cluster)
      
      (submit-local-topology (:nimbus cluster) "test3" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 5} topology)
      (bind storm-id3 (get-storm-id state "test3"))

      (bind executor-id (first (topology-executors cluster storm-id3)))
      
      (do-executor-heartbeat cluster storm-id3 executor-id)

      (.killTopology (:nimbus cluster) "test3")
      (advance-cluster-time cluster 6)
      (is (= 1 (count (.heartbeat-storms state))))
      (advance-cluster-time cluster 5)
      (is (= 0 (count (.heartbeat-storms state))))

      ;; test kill with opts
      (submit-local-topology (:nimbus cluster) "test4" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 100} topology)
      (.killTopologyWithOpts (:nimbus cluster) "test4" (doto (KillOptions.) (.set_wait_secs 10)))
      (bind storm-id4 (get-storm-id state "test4"))
      (advance-cluster-time cluster 9)
      (is (not-nil? (.assignment-info state storm-id4 nil)))
      (advance-cluster-time cluster 2)
      (is (nil? (.assignment-info state storm-id4 nil)))      
      )))

(deftest test-reassignment
  (with-simulated-time-local-cluster [cluster :supervisors 2 :ports-per-supervisor 5
    :daemon-conf {SUPERVISOR-ENABLE false
                  NIMBUS-TASK-LAUNCH-SECS 60
                  NIMBUS-TASK-TIMEOUT-SECS 20
                  NIMBUS-MONITOR-FREQ-SECS 10
                  NIMBUS-SUPERVISOR-TIMEOUT-SECS 100
                  TOPOLOGY-ACKER-EXECUTORS 0}]
    (letlocals
      (bind conf (:daemon-conf cluster))
      (bind topology (thrift/mk-topology
                       {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 2)}
                       {}
                       ))
      (bind state (:storm-cluster-state cluster))
      (submit-local-topology (:nimbus cluster) "test" {TOPOLOGY-WORKERS 2} topology)
      (check-consistency cluster "test")
      (bind storm-id (get-storm-id state "test"))
      (bind [executor-id1 executor-id2]  (topology-executors cluster storm-id))
      (bind ass1 (executor-assignment cluster storm-id executor-id1))
      (bind ass2 (executor-assignment cluster storm-id executor-id2))
      
      (advance-cluster-time cluster 59)
      (do-executor-heartbeat cluster storm-id executor-id1)
      (do-executor-heartbeat cluster storm-id executor-id2)

      (advance-cluster-time cluster 13)
      (is (= ass1 (executor-assignment cluster storm-id executor-id1)))
      (is (= ass2 (executor-assignment cluster storm-id executor-id2)))
      (do-executor-heartbeat cluster storm-id executor-id1)

      (advance-cluster-time cluster 11)
      (do-executor-heartbeat cluster storm-id executor-id1)
      (is (= ass1 (executor-assignment cluster storm-id executor-id1)))
      (check-consistency cluster "test")

      ; have to wait an extra 10 seconds because nimbus may not
      ; resynchronize its heartbeat time till monitor-time secs after
      (advance-cluster-time cluster 11)
      (do-executor-heartbeat cluster storm-id executor-id1)
      (is (= ass1 (executor-assignment cluster storm-id executor-id1)))
      (check-consistency cluster "test")
      
      (advance-cluster-time cluster 11)
      (is (= ass1 (executor-assignment cluster storm-id executor-id1)))
      (is (not= ass2 (executor-assignment cluster storm-id executor-id2)))
      (bind ass2 (executor-assignment cluster storm-id executor-id2))
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
                  TOPOLOGY-ACKER-EXECUTORS 0}]
    (letlocals
      (add-supervisor cluster :ports 1 :id "a")
      (add-supervisor cluster :ports 1 :id "b")
      (bind conf (:daemon-conf cluster))
      (bind topology (thrift/mk-topology
                       {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 2)}
                       {}
                       ))
      (bind state (:storm-cluster-state cluster))
      (submit-local-topology (:nimbus cluster) "test" {TOPOLOGY-WORKERS 2} topology)
      (check-consistency cluster "test")
      (bind storm-id (get-storm-id state "test"))
      (bind [executor-id1 executor-id2]  (topology-executors cluster storm-id))
      (bind ass1 (executor-assignment cluster storm-id executor-id1))
      (bind ass2 (executor-assignment cluster storm-id executor-id2))
      
      (advance-cluster-time cluster 59)
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
                  TOPOLOGY-ACKER-EXECUTORS 0}]
    (letlocals
      (bind topology (thrift/mk-topology
                        {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 9)}
                        {}))
      (bind state (:storm-cluster-state cluster))
      (submit-local-topology (:nimbus cluster) "test" {TOPOLOGY-WORKERS 4} topology)  ; distribution should be 2, 2, 2, 3 ideally
      (bind storm-id (get-storm-id state "test"))
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

      (bind common (first (find-first (fn [[k v]] (= 3 (count v))) slot-executors2)))
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
                  TOPOLOGY-ACKER-EXECUTORS 0}]
    (letlocals
      (bind topology (thrift/mk-topology
                        {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 3)}
                        {}))
      (bind state (:storm-cluster-state cluster))
      (submit-local-topology (:nimbus cluster)
                             "test"
                             {TOPOLOGY-WORKERS 3
                              TOPOLOGY-MESSAGE-TIMEOUT-SECS 60} topology)
      (bind storm-id (get-storm-id state "test"))
      (add-supervisor cluster :ports 3)
      (add-supervisor cluster :ports 3)

      (advance-cluster-time cluster 91)

      (bind slot-executors (slot-assignments cluster storm-id))
      ;; check that all workers are on one machine
      (check-executor-distribution slot-executors [1 1 1])
      (check-num-nodes slot-executors 1)
      (.rebalance (:nimbus cluster) "test" (RebalanceOptions.))

      (advance-cluster-time cluster 31)
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

(deftest test-rebalance-change-parallelism
  (with-simulated-time-local-cluster [cluster :supervisors 4 :ports-per-supervisor 3
    :daemon-conf {SUPERVISOR-ENABLE false
                  NIMBUS-MONITOR-FREQ-SECS 10
                  TOPOLOGY-ACKER-EXECUTORS 0}]
    (letlocals
      (bind topology (thrift/mk-topology
                        {"1" (thrift/mk-spout-spec (TestPlannerSpout. true)
                                :parallelism-hint 6
                                :conf {TOPOLOGY-TASKS 12})}
                        {}))
      (bind state (:storm-cluster-state cluster))
      (submit-local-topology (:nimbus cluster)
                             "test"
                             {TOPOLOGY-WORKERS 3
                              TOPOLOGY-MESSAGE-TIMEOUT-SECS 30} topology)
      (bind storm-id (get-storm-id state "test"))
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
                               (map-val #(map executor-id->tasks %))))
      (check-distribution (executor-info "1") [2 2 2 2 1 1 1 1])
      
      )))

(deftest test-submit-invalid
  (with-simulated-time-local-cluster [cluster
    :daemon-conf {SUPERVISOR-ENABLE false
                  TOPOLOGY-ACKER-EXECUTORS 0}]
    (letlocals
      (bind topology (thrift/mk-topology
                        {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 0 :conf {TOPOLOGY-TASKS 1})}
                        {}))
     
      (is (thrown? InvalidTopologyException
        (submit-local-topology (:nimbus cluster)
                               "test"
                               {}
                               topology)))
                               
      (bind topology (thrift/mk-topology
                        {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 1 :conf {TOPOLOGY-TASKS 1})}
                        {}))
      (is (thrown? InvalidTopologyException
        (submit-local-topology (:nimbus cluster)
                               "test/aaa"
                               {}
                               topology)))
      )))

(deftest test-cleans-corrupt
  (with-inprocess-zookeeper zk-port
    (with-local-tmp [nimbus-dir]
      (letlocals
       (bind conf (merge (read-storm-config)
                         {STORM-ZOOKEEPER-SERVERS ["localhost"]
                          STORM-CLUSTER-MODE "local"
                          STORM-ZOOKEEPER-PORT zk-port
                          STORM-LOCAL-DIR nimbus-dir}))
       (bind cluster-state (cluster/mk-storm-cluster-state conf))
       (bind nimbus (nimbus/service-handler conf (nimbus/standalone-nimbus)))
       (bind topology (thrift/mk-topology
                       {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 3)}
                       {}))
       (submit-local-topology nimbus "t1" {} topology)
       (submit-local-topology nimbus "t2" {} topology)
       (bind storm-id1 (get-storm-id cluster-state "t1"))
       (bind storm-id2 (get-storm-id cluster-state "t2"))
       (.shutdown nimbus)
       (rmr (master-stormdist-root conf storm-id1))
       (bind nimbus (nimbus/service-handler conf (nimbus/standalone-nimbus)))
       (is ( = #{storm-id2} (set (.active-storms cluster-state))))
       (.shutdown nimbus)
       (.disconnect cluster-state)
       ))))

(deftest test-no-overlapping-slots
  ;; test that same node+port never appears across 2 assignments
  )

(deftest test-stateless
  ;; test that nimbus can die and restart without any problems
  )

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

(deftest test-validate-topo-config-on-submit
  (with-local-cluster [cluster]
    (let [nimbus (:nimbus cluster)
          topology (thrift/mk-topology {} {})
          bad-config {"topology.workers" "3"}]
      (is (thrown-cause? InvalidTopologyException
        (submit-local-topology-with-opts nimbus "test" bad-config topology
                                         (SubmitOptions.)))))))
