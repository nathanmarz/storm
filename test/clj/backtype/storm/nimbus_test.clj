(ns backtype.storm.nimbus-test
  (:use [clojure test])
  (:use [clojure.contrib.def :only [defnk]])
  (:require [backtype.storm.daemon [nimbus :as nimbus]])
  
  (:import [backtype.storm.testing TestWordCounter TestWordSpout TestGlobalCount TestAggregatesCounter])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  )

(bootstrap)

(defn storm-component-info [state storm-name]
  (let [storm-id (get-storm-id state storm-name)]
    (reverse-map (storm-task-info state storm-id))))

(defn storm-num-workers [state storm-name]
  (let [storm-id (get-storm-id state storm-name)
        assignment (.assignment-info state storm-id nil)]
    (count (reverse-map (:task->node+port assignment)))
    ))

(defn do-task-heartbeat [cluster storm-id task-id]
  (let [state (:storm-cluster-state cluster)]
    (.task-heartbeat! state storm-id task-id (TaskHeartbeat. (current-time-secs) 10 {}))
    ))

(defn task-assignment [cluster storm-id task-id]
  (let [state (:storm-cluster-state cluster)
        assignment (.assignment-info state storm-id nil)]
    ((:task->node+port assignment) task-id)
    ))

(defn slot-assignments [cluster storm-id]
  (let [state (:storm-cluster-state cluster)
        assignment (.assignment-info state storm-id nil)]
    (reverse-map (:task->node+port assignment))
    ))

(defn task-start-times [cluster storm-id]
  (let [state (:storm-cluster-state cluster)
        assignment (.assignment-info state storm-id nil)]
    (:task->start-time-secs assignment)
    ))

(defnk check-consistency [cluster storm-name :assigned? true]
  (let [state (:storm-cluster-state cluster)
        storm-id (get-storm-id state storm-name)
        task-ids (.task-ids state storm-id)
        assignment (.assignment-info state storm-id nil)
        task->node+port (:task->node+port assignment)
        all-nodes (set (map first (vals task->node+port)))]
    (when assigned?
      (is (= (set task-ids) (set (keys task->node+port)))))
    (doseq [[t s] task->node+port]
      (is (not-nil? s)))
    (is (= all-nodes (set (keys (:node->host assignment)))))
    (doseq [[t s] task->node+port]
      (is (not-nil? ((:task->start-time-secs assignment) t))))
    ))

(deftest test-assignment
  (with-local-cluster [cluster :supervisors 4 :ports-per-supervisor 3 :daemon-conf {SUPERVISOR-ENABLE false TOPOLOGY-ACKERS 0}]
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
          task-info (storm-component-info state "mystorm")]
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
      (let [task-info (storm-component-info state "storm2")]
        (is (= 12 (count (task-info "1"))))
        (is (= 6 (count (task-info "2"))))
        (is (= 8 (count (task-info "3"))))
        (is (= 4 (count (task-info "4"))))
        (is (= 8 (storm-num-workers state "storm2")))
        )
      )))

(deftest test-over-parallelism-assignment
  (with-local-cluster [cluster :supervisors 2 :ports-per-supervisor 5 :daemon-conf {SUPERVISOR-ENABLE false TOPOLOGY-ACKERS 0}]
    (let [state (:storm-cluster-state cluster)
          nimbus (:nimbus cluster)
          topology (thrift/mk-topology
                     {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 21)}
                     {"2" (thrift/mk-bolt-spec {"1" :none} (TestPlannerBolt.) :parallelism-hint 9)
                      "3" (thrift/mk-bolt-spec {"1" :none} (TestPlannerBolt.) :parallelism-hint 2)
                      "4" (thrift/mk-bolt-spec {"1" :none} (TestPlannerBolt.) :parallelism-hint 10)}
                     )
          _ (submit-local-topology nimbus "test" {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 7} topology)
          task-info (storm-component-info state "test")]
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
                  TOPOLOGY-ACKERS 0}]
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
      (is (= 1 (count (.task-storms state))))
      (is (= 1 (count (.heartbeat-storms state))))
      (advance-cluster-time cluster 3)
      (is (nil? (.storm-base state storm-id nil)))
      (is (nil? (.assignment-info state storm-id nil)))
      (is (empty? (.task-storms state)))

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
      (is (= 1 (count (.task-storms state))))
      (is (= 1 (count (.heartbeat-storms state))))
      
      (advance-cluster-time cluster 6)
      (is (nil? (.storm-base state storm-id nil)))
      (is (nil? (.assignment-info state storm-id nil)))
      (advance-cluster-time cluster 11)
      (is (= 0 (count (.task-storms state))))
      (is (= 0 (count (.heartbeat-storms state))))
      
      (submit-local-topology (:nimbus cluster) "test3" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 5} topology)
      (bind storm-id3 (get-storm-id state "test3"))
      (advance-cluster-time cluster 1)
      (.remove-storm! state storm-id3)
      (is (nil? (.storm-base state storm-id3 nil)))
      (is (nil? (.assignment-info state storm-id3 nil)))

      (advance-cluster-time cluster 11)
      (is (= 0 (count (.task-storms state))))
      (is (= 0 (count (.heartbeat-storms state))))

      ;; this guarantees that monitor thread won't trigger for 10 more seconds
      (advance-time-secs! 11)
      (wait-until-cluster-waiting cluster)
      
      (submit-local-topology (:nimbus cluster) "test3" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 5} topology)
      (bind storm-id3 (get-storm-id state "test3"))
      (bind task-id (first (.task-ids state storm-id3)))
      (do-task-heartbeat cluster storm-id3 task-id)
      (.killTopology (:nimbus cluster) "test3")
      (advance-cluster-time cluster 6)
      (is (= 0 (count (.task-storms state))))
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
                  TOPOLOGY-ACKERS 0}]
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
      (bind [task-id1 task-id2]  (.task-ids state storm-id))
      (bind ass1 (task-assignment cluster storm-id task-id1))
      (bind ass2 (task-assignment cluster storm-id task-id2))

      (advance-cluster-time cluster 59)
      (do-task-heartbeat cluster storm-id task-id1)
      (do-task-heartbeat cluster storm-id task-id2)

      (advance-cluster-time cluster 13)
      (is (= ass1 (task-assignment cluster storm-id task-id1)))
      (is (= ass2 (task-assignment cluster storm-id task-id2)))
      (do-task-heartbeat cluster storm-id task-id1)

      (advance-cluster-time cluster 11)
      (do-task-heartbeat cluster storm-id task-id1)
      (is (= ass1 (task-assignment cluster storm-id task-id1)))
      (check-consistency cluster "test")

      ; have to wait an extra 10 seconds because nimbus may not
      ; resynchronize its heartbeat time till monitor-time secs after
      (advance-cluster-time cluster 11)
      (do-task-heartbeat cluster storm-id task-id1)
      (is (= ass1 (task-assignment cluster storm-id task-id1)))
      (check-consistency cluster "test")
      
      (advance-cluster-time cluster 11)
      (is (= ass1 (task-assignment cluster storm-id task-id1)))
      (is (not= ass2 (task-assignment cluster storm-id task-id2)))
      (bind ass2 (task-assignment cluster storm-id task-id2))
      (check-consistency cluster "test")

      (advance-cluster-time cluster 31)
      (is (not= ass1 (task-assignment cluster storm-id task-id1)))
      (is (= ass2 (task-assignment cluster storm-id task-id2)))  ; tests launch timeout
      (check-consistency cluster "test")


      (bind ass1 (task-assignment cluster storm-id task-id1))
      (bind active-supervisor (first ass2))
      (kill-supervisor cluster active-supervisor)

      (doseq [i (range 12)]
        (do-task-heartbeat cluster storm-id task-id1)
        (do-task-heartbeat cluster storm-id task-id2)
        (advance-cluster-time cluster 10)
        )
      ;; tests that it doesn't reassign tasks if they're heartbeating even if supervisor times out
      (is (= ass1 (task-assignment cluster storm-id task-id1)))
      (is (= ass2 (task-assignment cluster storm-id task-id2)))
      (check-consistency cluster "test")

      (advance-cluster-time cluster 30)

      (bind ass1 (task-assignment cluster storm-id task-id1))
      (bind ass2 (task-assignment cluster storm-id task-id2))
      (is (not-nil? ass1))
      (is (not-nil? ass2))
      (is (not= active-supervisor (first (task-assignment cluster storm-id task-id2))))
      (is (not= active-supervisor (first (task-assignment cluster storm-id task-id1))))
      (check-consistency cluster "test")

      (doseq [supervisor-id (.supervisors state nil)]
        (kill-supervisor cluster supervisor-id))

      (advance-cluster-time cluster 90)
      (bind ass1 (task-assignment cluster storm-id task-id1))
      (bind ass2 (task-assignment cluster storm-id task-id2))
      (is (nil? ass1))
      (is (nil? ass2))
      (check-consistency cluster "test" :assigned? false)

      (add-supervisor cluster)
      (advance-cluster-time cluster 11)
      (check-consistency cluster "test")
      )))

(defn check-distribution [slot-tasks distribution]
  (let [dist (multi-set (map count (vals slot-tasks)))]
    (is (= dist (multi-set distribution)))
    ))

(defn check-num-nodes [slot-tasks num-nodes]
  (let [nodes (->> slot-tasks keys (map first) set)]
    (is (= num-nodes (count nodes)))
    ))

(deftest test-reassign-squeezed-topology
  (with-simulated-time-local-cluster [cluster :supervisors 1 :ports-per-supervisor 1
    :daemon-conf {SUPERVISOR-ENABLE false
                  NIMBUS-TASK-LAUNCH-SECS 60
                  NIMBUS-TASK-TIMEOUT-SECS 20
                  NIMBUS-MONITOR-FREQ-SECS 10
                  TOPOLOGY-ACKERS 0}]
    (letlocals
      (bind topology (thrift/mk-topology
                        {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 9)}
                        {}))
      (bind state (:storm-cluster-state cluster))
      (submit-local-topology (:nimbus cluster) "test" {TOPOLOGY-WORKERS 4} topology)  ; distribution should be 2, 2, 2, 3 ideally
      (bind storm-id (get-storm-id state "test"))
      (bind slot-tasks (slot-assignments cluster storm-id))
      (check-distribution (slot-assignments cluster storm-id) [9])
      (check-consistency cluster "test")

      (add-supervisor cluster :ports 2)
      (advance-cluster-time cluster 11)
      (bind slot-tasks (slot-assignments cluster storm-id))
      (bind task->start (task-start-times cluster storm-id))
      (check-distribution slot-tasks [3 3 3])
      (check-consistency cluster "test")

      (add-supervisor cluster :ports 8)
      ;; this actually works for any time > 0, since zookeeper fires an event causing immediate reassignment
      ;; doesn't work for time = 0 because it's not waiting for cluster yet, so test might happen before reassignment finishes
      (advance-cluster-time cluster 11)
      (bind slot-tasks2 (slot-assignments cluster storm-id))
      (bind task->start2 (task-start-times cluster storm-id))
      (check-distribution slot-tasks2 [2 2 2 3])
      (check-consistency cluster "test")

      (bind common (first (find-first (fn [[k v]] (= 3 (count v))) slot-tasks2)))
      (is (not-nil? common))
      (is (= (slot-tasks2 common) (slot-tasks common)))
      
      ;; check that start times are changed for everything but the common one
      (bind same-tasks (slot-tasks2 common))
      (bind changed-tasks (apply concat (vals (dissoc slot-tasks2 common))))
      (doseq [t same-tasks]
        (is (= (task->start t) (task->start2 t))))
      (doseq [t changed-tasks]
        (is (not= (task->start t) (task->start2 t))))
      )))

(deftest test-rebalance
  (with-simulated-time-local-cluster [cluster :supervisors 1 :ports-per-supervisor 3
    :daemon-conf {SUPERVISOR-ENABLE false
                  NIMBUS-MONITOR-FREQ-SECS 10
                  TOPOLOGY-MESSAGE-TIMEOUT-SECS 30
                  TOPOLOGY-ACKERS 0}]
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

      (bind slot-tasks (slot-assignments cluster storm-id))
      ;; check that all workers are on one machine
      (check-distribution slot-tasks [1 1 1])
      (check-num-nodes slot-tasks 1)
      (.rebalance (:nimbus cluster) "test" (RebalanceOptions.))

      (advance-cluster-time cluster 31)
      (check-distribution slot-tasks [1 1 1])
      (check-num-nodes slot-tasks 1)


      (advance-cluster-time cluster 30)
      (bind slot-tasks (slot-assignments cluster storm-id))
      (check-distribution slot-tasks [1 1 1])
      (check-num-nodes slot-tasks 3)
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
       (bind nimbus (nimbus/service-handler conf))
       (bind topology (thrift/mk-topology
                       {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 3)}
                       {}))
       (submit-local-topology nimbus "t1" {} topology)
       (submit-local-topology nimbus "t2" {} topology)
       (bind storm-id1 (get-storm-id cluster-state "t1"))
       (bind storm-id2 (get-storm-id cluster-state "t2"))
       (.shutdown nimbus)
       (rmr (master-stormdist-root conf storm-id1))
       (bind nimbus (nimbus/service-handler conf))
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
       )))

  )
