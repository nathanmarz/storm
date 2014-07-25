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
(ns backtype.storm.supervisor-test
  (:use [clojure test])
  (:require [clojure [string :as string]])
  (:import [backtype.storm.testing TestWordCounter TestWordSpout TestGlobalCount TestAggregatesCounter])
  (:use [backtype.storm bootstrap config testing])
  (:use [backtype.storm.daemon common])
  (:require [backtype.storm.daemon [worker :as worker] [supervisor :as supervisor]])
  (:use [conjure core])
  )

(bootstrap)


(defn worker-assignment
  "Return [storm-id executors]"
  [cluster supervisor-id port]
  (let [state (:storm-cluster-state cluster)
        slot-assigns (for [storm-id (.assignments state nil)]
                        (let [executors (-> (.assignment-info state storm-id nil)
                                        :executor->node+port
                                        reverse-map
                                        (get [supervisor-id port] ))]
                          (when executors [storm-id executors])
                          ))
        ret (find-first not-nil? slot-assigns)]
    (when-not ret
      (throw-runtime "Could not find assignment for worker"))
    ret
    ))

(defn heartbeat-worker [supervisor port storm-id executors]
  (let [conf (.get-conf supervisor)]
    (worker/do-heartbeat {:conf conf
                          :port port
                          :storm-id storm-id
                          :executors executors
                          :worker-id (find-worker-id conf port)})))

(defn heartbeat-workers [cluster supervisor-id ports]
  (let [sup (get-supervisor cluster supervisor-id)]
    (doseq [p ports]
      (let [[storm-id executors] (worker-assignment cluster supervisor-id p)]
        (heartbeat-worker sup p storm-id executors)
        ))))

(defn validate-launched-once [launched supervisor->ports storm-id]
  (let [counts (map count (vals launched))
        launched-supervisor->ports (apply merge-with set/union
                                          (for [[[s p] sids] launched
                                                :when (some #(= % storm-id) sids)]
                                            {s #{p}}))
        supervisor->ports (map-val set supervisor->ports)]
    (is (every? (partial = 1) counts))
    (is (= launched-supervisor->ports supervisor->ports))
    ))

(deftest launches-assignment
  (with-simulated-time-local-cluster [cluster :supervisors 0
    :daemon-conf {NIMBUS-REASSIGN false
                  SUPERVISOR-WORKER-START-TIMEOUT-SECS 5
                  SUPERVISOR-WORKER-TIMEOUT-SECS 15
                  SUPERVISOR-MONITOR-FREQUENCY-SECS 3}]
    (letlocals
      (bind topology (thrift/mk-topology
                       {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 4)}
                       {}))
      (bind sup1 (add-supervisor cluster :id "sup1" :ports [1 2 3 4]))
      (bind changed (capture-changed-workers
                        (submit-mocked-assignment
                          (:nimbus cluster)
                          "test"
                          {TOPOLOGY-WORKERS 3}
                          topology
                          {1 "1"
                           2 "1"
                           3 "1"
                           4 "1"}
                          {[1] ["sup1" 1]
                           [2] ["sup1" 2]
                           [3] ["sup1" 3]
                           [4] ["sup1" 3]
                           })
                        (advance-cluster-time cluster 2)
                        (heartbeat-workers cluster "sup1" [1 2 3])
                        (advance-cluster-time cluster 10)))
      (bind storm-id (get-storm-id (:storm-cluster-state cluster) "test"))
      (is (empty? (:shutdown changed)))
      (validate-launched-once (:launched changed) {"sup1" [1 2 3]} storm-id)
      (bind changed (capture-changed-workers
                        (doseq [i (range 10)]
                          (heartbeat-workers cluster "sup1" [1 2 3])
                          (advance-cluster-time cluster 10))
                        ))
      (is (empty? (:shutdown changed)))
      (is (empty? (:launched changed)))
      (bind changed (capture-changed-workers
                      (heartbeat-workers cluster "sup1" [1 2])
                      (advance-cluster-time cluster 10)
                      ))
      (validate-launched-once (:launched changed) {"sup1" [3]} storm-id)
      (is (= {["sup1" 3] 1} (:shutdown changed)))
      )))

(deftest test-multiple-active-storms-multiple-supervisors
  (with-simulated-time-local-cluster [cluster :supervisors 0
    :daemon-conf {NIMBUS-REASSIGN false
                  SUPERVISOR-WORKER-START-TIMEOUT-SECS 5
                  SUPERVISOR-WORKER-TIMEOUT-SECS 15
                  SUPERVISOR-MONITOR-FREQUENCY-SECS 3}]
    (letlocals
      (bind topology (thrift/mk-topology
                       {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 4)}
                       {}))
      (bind topology2 (thrift/mk-topology
                       {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 3)}
                       {}))
      (bind sup1 (add-supervisor cluster :id "sup1" :ports [1 2 3 4]))
      (bind sup2 (add-supervisor cluster :id "sup2" :ports [1 2]))
      (bind changed (capture-changed-workers
                        (submit-mocked-assignment
                          (:nimbus cluster)
                          "test"
                          {TOPOLOGY-WORKERS 3 TOPOLOGY-MESSAGE-TIMEOUT-SECS 40}
                          topology
                          {1 "1"
                           2 "1"
                           3 "1"
                           4 "1"}
                          {[1] ["sup1" 1]
                           [2] ["sup1" 2]
                           [3] ["sup2" 1]
                           [4] ["sup2" 1]
                           })
                        (advance-cluster-time cluster 2)
                        (heartbeat-workers cluster "sup1" [1 2])
                        (heartbeat-workers cluster "sup2" [1])
                        ))
      (bind storm-id (get-storm-id (:storm-cluster-state cluster) "test"))
      (is (empty? (:shutdown changed)))
      (validate-launched-once (:launched changed) {"sup1" [1 2] "sup2" [1]} storm-id)
      (bind changed (capture-changed-workers
                        (submit-mocked-assignment
                          (:nimbus cluster)
                          "test2"
                          {TOPOLOGY-WORKERS 2}
                          topology2
                          {1 "1"
                           2 "1"
                           3 "1"}
                          {[1] ["sup1" 3]
                           [2] ["sup1" 3]
                           [3] ["sup2" 2]
                           })
                        (advance-cluster-time cluster 2)
                        (heartbeat-workers cluster "sup1" [3])
                        (heartbeat-workers cluster "sup2" [2])
                        ))
      (bind storm-id2 (get-storm-id (:storm-cluster-state cluster) "test2"))
      (is (empty? (:shutdown changed)))
      (validate-launched-once (:launched changed) {"sup1" [3] "sup2" [2]} storm-id2)
      (bind changed (capture-changed-workers
        (.killTopology (:nimbus cluster) "test")
        (doseq [i (range 4)]
          (advance-cluster-time cluster 8)
          (heartbeat-workers cluster "sup1" [1 2 3])
          (heartbeat-workers cluster "sup2" [1 2])
          )))
      (is (empty? (:shutdown changed)))
      (is (empty? (:launched changed)))
      (bind changed (capture-changed-workers
        (advance-cluster-time cluster 12)
        ))
      (is (empty? (:launched changed)))
      (is (= {["sup1" 1] 1 ["sup1" 2] 1 ["sup2" 1] 1} (:shutdown changed)))
      (bind changed (capture-changed-workers
        (doseq [i (range 10)]
          (heartbeat-workers cluster "sup1" [3])
          (heartbeat-workers cluster "sup2" [2])
          (advance-cluster-time cluster 10)
          )))
      (is (empty? (:shutdown changed)))
      (is (empty? (:launched changed)))
      ;; TODO check that downloaded code is cleaned up only for the one storm
      )))

(defn get-heartbeat [cluster supervisor-id]
  (.supervisor-info (:storm-cluster-state cluster) supervisor-id))

(defn check-heartbeat [cluster supervisor-id within-secs]
  (let [hb (get-heartbeat cluster supervisor-id)
        time-secs (:time-secs hb)
        now (current-time-secs)
        delta (- now time-secs)]
    (is (>= delta 0))
    (is (<= delta within-secs))
    ))

(deftest heartbeats-to-nimbus
  (with-simulated-time-local-cluster [cluster :supervisors 0
    :daemon-conf {SUPERVISOR-WORKER-START-TIMEOUT-SECS 15
                  SUPERVISOR-HEARTBEAT-FREQUENCY-SECS 3}]
    (letlocals
      (bind sup1 (add-supervisor cluster :id "sup" :ports [5 6 7]))
      (advance-cluster-time cluster 4)
      (bind hb (get-heartbeat cluster "sup"))
      (is (= #{5 6 7} (set (:meta hb))))
      (check-heartbeat cluster "sup" 3)
      (advance-cluster-time cluster 3)
      (check-heartbeat cluster "sup" 3)
      (advance-cluster-time cluster 3)
      (check-heartbeat cluster "sup" 3)
      (advance-cluster-time cluster 15)
      (check-heartbeat cluster "sup" 3)
      (bind topology (thrift/mk-topology
                       {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 4)}
                       {}))
      ;; prevent them from launching by capturing them
      (capture-changed-workers
       (submit-local-topology (:nimbus cluster) "test" {TOPOLOGY-WORKERS 2} topology)
       (advance-cluster-time cluster 3)
       (check-heartbeat cluster "sup" 3)
       (advance-cluster-time cluster 3)
       (check-heartbeat cluster "sup" 3)
       (advance-cluster-time cluster 3)
       (check-heartbeat cluster "sup" 3)
       (advance-cluster-time cluster 20)
       (check-heartbeat cluster "sup" 3))
      )))

(deftest test-worker-launch-command
  (testing "*.worker.childopts configuration"
    (let [mock-port "42"
          mock-storm-id "fake-storm-id"
          mock-worker-id "fake-worker-id"
          mock-cp "/base:/stormjar.jar"
          exp-args-fn (fn [opts topo-opts classpath]
                       (concat [(supervisor/java-cmd) "-server"]
                               opts
                               topo-opts
                               ["-Djava.library.path="
                                (str "-Dlogfile.name=worker-" mock-port ".log")
                                "-Dstorm.home="
                                "-Dlogback.configurationFile=/logback/cluster.xml"
                                (str "-Dstorm.id=" mock-storm-id)
                                (str "-Dworker.id=" mock-worker-id)
                                (str "-Dworker.port=" mock-port)
                                "-cp" classpath
                                "backtype.storm.daemon.worker"
                                mock-storm-id
                                mock-port
                                mock-worker-id]))]
      (testing "testing *.worker.childopts as strings with extra spaces"
        (let [string-opts "-Dfoo=bar  -Xmx1024m"
              topo-string-opts "-Dkau=aux   -Xmx2048m"
              exp-args (exp-args-fn ["-Dfoo=bar" "-Xmx1024m"]
                                    ["-Dkau=aux" "-Xmx2048m"]
                                    mock-cp)
              mock-supervisor {:conf {STORM-CLUSTER-MODE :distributed
                                      WORKER-CHILDOPTS string-opts}}]
          (stubbing [read-supervisor-storm-conf {TOPOLOGY-WORKER-CHILDOPTS
                                                   topo-string-opts}
                     add-to-classpath mock-cp
                     supervisor-stormdist-root nil
                     supervisor/jlp nil
                     launch-process nil]
            (supervisor/launch-worker mock-supervisor
                                      mock-storm-id
                                      mock-port
                                      mock-worker-id)
            (verify-first-call-args-for-indices launch-process
                                                [0]
                                                exp-args))))
      (testing "testing *.worker.childopts as list of strings, with spaces in values"
        (let [list-opts '("-Dopt1='this has a space in it'" "-Xmx1024m")
              topo-list-opts '("-Dopt2='val with spaces'" "-Xmx2048m")
              exp-args (exp-args-fn list-opts topo-list-opts mock-cp)
              mock-supervisor {:conf {STORM-CLUSTER-MODE :distributed
                                      WORKER-CHILDOPTS list-opts}}]
          (stubbing [read-supervisor-storm-conf {TOPOLOGY-WORKER-CHILDOPTS
                                                   topo-list-opts}
                     add-to-classpath mock-cp
                     supervisor-stormdist-root nil
                     supervisor/jlp nil
                     launch-process nil]
            (supervisor/launch-worker mock-supervisor
                                      mock-storm-id
                                      mock-port
                                      mock-worker-id)
            (verify-first-call-args-for-indices launch-process
                                                [0]
                                                exp-args))))
      (testing "testing topology.classpath is added to classpath"
        (let [topo-cp "/any/path"
              exp-args (exp-args-fn [] [] (add-to-classpath mock-cp [topo-cp]))
              mock-supervisor {:conf {STORM-CLUSTER-MODE :distributed}}]
          (stubbing [read-supervisor-storm-conf {TOPOLOGY-CLASSPATH topo-cp}
                     supervisor-stormdist-root nil
                     supervisor/jlp nil
                     launch-process nil
                     current-classpath "/base"]
                    (supervisor/launch-worker mock-supervisor
                                              mock-storm-id
                                              mock-port
                                              mock-worker-id)
                    (verify-first-call-args-for-indices launch-process
                                                        [0]
                                                        exp-args))))
      (testing "testing topology.environment is added to environment for worker launch"
        (let [topo-env {"THISVAR" "somevalue" "THATVAR" "someothervalue"}
              exp-args (exp-args-fn [] [] mock-cp)
              mock-supervisor {:conf {STORM-CLUSTER-MODE :distributed}}]
          (stubbing [read-supervisor-storm-conf {TOPOLOGY-ENVIRONMENT topo-env}
                     supervisor-stormdist-root nil
                     supervisor/jlp nil
                     launch-process nil
                     current-classpath "/base"]
                    (supervisor/launch-worker mock-supervisor
                                              mock-storm-id
                                              mock-port
                                              mock-worker-id)
                    (verify-first-call-args-for-indices launch-process
                                                        [2]
                                                        (merge topo-env {"LD_LIBRARY_PATH" nil}))))))))

(deftest test-workers-go-bananas
  ;; test that multiple workers are started for a port, and test that
  ;; supervisor shuts down propertly (doesn't shutdown the most
  ;; recently launched one, checks heartbeats correctly, etc.)
  )

(deftest downloads-code
  )

(deftest test-stateless
  )

(deftest cleans-up-on-unassign
  ;; TODO just do reassign, and check that cleans up worker states after killing but doesn't get rid of downloaded code
  )

(deftest test-retry-read-assignments
  (with-simulated-time-local-cluster [cluster
                                      :supervisors 0
                                      :ports-per-supervisor 2
                                      :daemon-conf {NIMBUS-REASSIGN false
                                                    NIMBUS-MONITOR-FREQ-SECS 10
                                                    TOPOLOGY-MESSAGE-TIMEOUT-SECS 30
                                                    TOPOLOGY-ACKER-EXECUTORS 0}]
    (letlocals
     (bind sup1 (add-supervisor cluster :id "sup1" :ports [1 2 3 4]))
     (bind topology1 (thrift/mk-topology
                      {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 2)}
                      {}))
     (bind topology2 (thrift/mk-topology
                      {"1" (thrift/mk-spout-spec (TestPlannerSpout. true) :parallelism-hint 2)}
                      {}))
     (bind state (:storm-cluster-state cluster))
     (bind changed (capture-changed-workers
                    (submit-mocked-assignment
                     (:nimbus cluster)
                     "topology1"
                     {TOPOLOGY-WORKERS 2}
                     topology1
                     {1 "1"
                      2 "1"}
                     {[1] ["sup1" 1]
                      [2] ["sup1" 2]
                      })
                    (submit-mocked-assignment
                     (:nimbus cluster)
                     "topology2"
                     {TOPOLOGY-WORKERS 2}
                     topology2
                     {1 "1"
                      2 "1"}
                     {[1] ["sup1" 1]
                      [2] ["sup1" 2]
                      })
                    (advance-cluster-time cluster 10)
                    ))
     (is (empty? (:launched changed)))
     (bind options (RebalanceOptions.))
     (.set_wait_secs options 0)
     (bind changed (capture-changed-workers
                    (.rebalance (:nimbus cluster) "topology2" options)
                    (advance-cluster-time cluster 10)
                    (heartbeat-workers cluster "sup1" [1 2 3 4])
                    (advance-cluster-time cluster 10)
                    ))
     (validate-launched-once (:launched changed)
                             {"sup1" [1 2]}
                             (get-storm-id (:storm-cluster-state cluster) "topology1"))
     (validate-launched-once (:launched changed)
                             {"sup1" [3 4]}
                             (get-storm-id (:storm-cluster-state cluster) "topology2"))
     )))
