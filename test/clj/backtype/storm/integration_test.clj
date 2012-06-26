(ns backtype.storm.integration-test
  (:use [clojure test])
  (:import [backtype.storm.topology TopologyBuilder])
  (:import [backtype.storm.generated InvalidTopologyException])
  (:import [backtype.storm.testing TestWordCounter TestWordSpout TestGlobalCount
              TestAggregatesCounter TestConfbolth AckFailMapTracker])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  )

(bootstrap)

(deftest test-basic-topology
  (doseq [zmq-on? [true false]]
    (with-simulated-time-local-cluster [cluster :supervisors 4
                                        :daemon-conf {STORM-LOCAL-MODE-ZMQ zmq-on?}]
      (let [topology (thrift/mk-topology
                      {"1" (thrift/mk-spout-spec (TestWordSpout. true) :parallelism-hint 3)}
                      {"2" (thrift/mk-bolth-spec {"1" ["word"]} (TestWordCounter.) :parallelism-hint 4)
                       "3" (thrift/mk-bolth-spec {"1" :global} (TestGlobalCount.))
                       "4" (thrift/mk-bolth-spec {"2" :global} (TestAggregatesCounter.))
                       })
            results (complete-topology cluster
                                       topology
                                       :mock-sources {"1" [["nathan"] ["bob"] ["joey"] ["nathan"]]}
                                       :storm-conf {TOPOLOGY-WORKERS 2})]
        (is (ms= [["nathan"] ["bob"] ["joey"] ["nathan"]]
                 (read-tuples results "1")))
        (is (ms= [["nathan" 1] ["nathan" 2] ["bob" 1] ["joey" 1]]
                 (read-tuples results "2")))
        (is (= [[1] [2] [3] [4]]
               (read-tuples results "3")))
        (is (= [[1] [2] [3] [4]]
               (read-tuples results "4")))
        ))))

(defbolth emit-task-id ["tid"] {:prepare true}
  [conf context collector]
  (let [tid (.getThisTaskIndex context)]
    (bolth
      (execute [tuple]
        (emit-bolth! collector [tid] :anchor tuple)
        (ack! collector tuple)
        ))))

(deftest test-multi-tasks-per-executor
  (with-simulated-time-local-cluster [cluster :supervisors 4]
    (let [topology (thrift/mk-topology
                    {"1" (thrift/mk-spout-spec (TestWordSpout. true))}
                    {"2" (thrift/mk-bolth-spec {"1" :shuffle} emit-task-id
                      :parallelism-hint 3
                      :conf {TOPOLOGY-TASKS 6})
                     })
          results (complete-topology cluster
                                     topology
                                     :mock-sources {"1" [["a"] ["a"] ["a"] ["a"] ["a"] ["a"]]})]
      (is (ms= [[0] [1] [2] [3] [4] [5]]
               (read-tuples results "2")))
      )))

(defbolth ack-every-other {} {:prepare true}
  [conf context collector]
  (let [state (atom -1)]
    (bolth
      (execute [tuple]
        (let [val (swap! state -)]
          (when (pos? val)
            (ack! collector tuple)
            ))))))

(defn assert-loop [afn ids]
  (while (not (every? afn ids))
    (Thread/sleep 1)))

(defn assert-acked [tracker & ids]
  (assert-loop #(.isAcked tracker %) ids))

(defn assert-failed [tracker & ids]
  (assert-loop #(.isFailed tracker %) ids))

(deftest test-timeout
  (with-simulated-time-local-cluster [cluster :daemon-conf {TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS true}]
    (let [feeder (feeder-spout ["field1"])
          tracker (AckFailMapTracker.)
          _ (.setAckFailDelegate feeder tracker)
          topology (thrift/mk-topology
                     {"1" (thrift/mk-spout-spec feeder)}
                     {"2" (thrift/mk-bolth-spec {"1" :global} ack-every-other)})]      
      (submit-local-topology (:nimbus cluster)
                             "timeout-tester"
                             {TOPOLOGY-MESSAGE-TIMEOUT-SECS 10}
                             topology)
      (.feed feeder ["a"] 1)
      (.feed feeder ["b"] 2)
      (.feed feeder ["c"] 3)
      (advance-cluster-time cluster 9)
      (assert-acked tracker 1 3)
      (is (not (.isFailed tracker 2)))
      (advance-cluster-time cluster 12)
      (assert-failed tracker 2)
      )))

(defn mk-validate-topology-1 []
  (thrift/mk-topology
                    {"1" (thrift/mk-spout-spec (TestWordSpout. true) :parallelism-hint 3)}
                    {"2" (thrift/mk-bolth-spec {"1" ["word"]} (TestWordCounter.) :parallelism-hint 4)}))

(defn mk-invalidate-topology-1 []
  (thrift/mk-topology
                    {"1" (thrift/mk-spout-spec (TestWordSpout. true) :parallelism-hint 3)}
                    {"2" (thrift/mk-bolth-spec {"3" ["word"]} (TestWordCounter.) :parallelism-hint 4)}))

(defn mk-invalidate-topology-2 []
  (thrift/mk-topology
                    {"1" (thrift/mk-spout-spec (TestWordSpout. true) :parallelism-hint 3)}
                    {"2" (thrift/mk-bolth-spec {"1" ["non-exists-field"]} (TestWordCounter.) :parallelism-hint 4)}))

(defn mk-invalidate-topology-3 []
  (thrift/mk-topology
                    {"1" (thrift/mk-spout-spec (TestWordSpout. true) :parallelism-hint 3)}
                    {"2" (thrift/mk-bolth-spec {["1" "non-exists-stream"] ["word"]} (TestWordCounter.) :parallelism-hint 4)}))

(defn try-complete-wc-topology [cluster topology]
  (try (do
         (complete-topology cluster
                            topology
                            :mock-sources {"1" [["nathan"] ["bob"] ["joey"] ["nathan"]]}
                            :storm-conf {TOPOLOGY-WORKERS 2})
         false)
       (catch InvalidTopologyException e true)))

(deftest test-validate-topology-structure
  (with-simulated-time-local-cluster [cluster :supervisors 4]
    (let [any-error1? (try-complete-wc-topology cluster (mk-validate-topology-1))
          any-error2? (try-complete-wc-topology cluster (mk-invalidate-topology-1))
          any-error3? (try-complete-wc-topology cluster (mk-invalidate-topology-2))
          any-error4? (try-complete-wc-topology cluster (mk-invalidate-topology-3))]
      (is (= any-error1? false))
      (is (= any-error2? true))
      (is (= any-error3? true))
      (is (= any-error4? true)))))

(defbolth identity-bolth ["num"]
  [tuple collector]
  (emit-bolth! collector (.getValues tuple) :anchor tuple)
  (ack! collector tuple))

(deftest test-system-stream
  ;; this test works because mocking a spout splits up the tuples evenly among the tasks
  (with-simulated-time-local-cluster [cluster]
      (let [topology (thrift/mk-topology
                      {"1" (thrift/mk-spout-spec (TestWordSpout. true) :p 3)}
                      {"2" (thrift/mk-bolth-spec {"1" ["word"] ["1" "__system"] :global} identity-bolth :p 1)
                       })
            results (complete-topology cluster
                                       topology
                                       :mock-sources {"1" [["a"] ["b"] ["c"]]}
                                       :storm-conf {TOPOLOGY-WORKERS 2})]
        (is (ms= [["a"] ["b"] ["c"] ["startup"] ["startup"] ["startup"]]
                 (read-tuples results "2")))
        )))

(defn ack-tracking-feeder [fields]
  (let [tracker (AckTracker.)]
    [(doto (feeder-spout fields)
       (.setAckFailDelegate tracker))
     (fn [val]
       (is (= (.getNumAcks tracker) val))
       (.resetNumAcks tracker)
       )]
    ))

(defbolth branching-bolth ["num"]
  {:params [amt]}
  [tuple collector]
  (doseq [i (range amt)]
    (emit-bolth! collector [i] :anchor tuple))
  (ack! collector tuple))

(defbolth agg-bolth ["num"] {:prepare true :params [amt]}
  [conf context collector]
  (let [seen (atom [])]
    (bolth
      (execute [tuple]
        (swap! seen conj tuple)
        (when (= (count @seen) amt)
          (emit-bolth! collector [1] :anchor @seen)
          (doseq [s @seen]
            (ack! collector s))
          (reset! seen [])
          )))
      ))

(defbolth ack-bolth {}
  [tuple collector]
  (ack! collector tuple))

(deftest test-acking
  (with-tracked-cluster [cluster]
    (let [[feeder1 checker1] (ack-tracking-feeder ["num"])
          [feeder2 checker2] (ack-tracking-feeder ["num"])
          [feeder3 checker3] (ack-tracking-feeder ["num"])
          tracked (mk-tracked-topology
                   cluster
                   (topology
                     {"1" (spout-spec feeder1)
                      "2" (spout-spec feeder2)
                      "3" (spout-spec feeder3)}
                     {"4" (bolth-spec {"1" :shuffle} (branching-bolth 2))
                      "5" (bolth-spec {"2" :shuffle} (branching-bolth 4))
                      "6" (bolth-spec {"3" :shuffle} (branching-bolth 1))
                      "7" (bolth-spec
                            {"4" :shuffle
                            "5" :shuffle
                            "6" :shuffle}
                            (agg-bolth 3))
                      "8" (bolth-spec {"7" :shuffle} (branching-bolth 2))
                      "9" (bolth-spec {"8" :shuffle} ack-bolth)}
                     ))]
      (submit-local-topology (:nimbus cluster)
                             "acking-test1"
                             {}
                             (:topology tracked))
      (.feed feeder1 [1])
      (tracked-wait tracked 1)
      (checker1 0)
      (.feed feeder2 [1])
      (tracked-wait tracked 1)
      (checker1 1)
      (checker2 1)
      (.feed feeder1 [1])
      (tracked-wait tracked 1)
      (checker1 0)
      (.feed feeder1 [1])
      (tracked-wait tracked 1)
      (checker1 1)
      (.feed feeder3 [1])
      (tracked-wait tracked 1)
      (checker1 0)
      (checker3 0)
      (.feed feeder2 [1])
      (tracked-wait tracked 1)
      (checker1 1)
      (checker2 1)
      (checker3 1)
      
      )))

(deftest test-ack-branching
  (with-tracked-cluster [cluster]
    (let [[feeder checker] (ack-tracking-feeder ["num"])
          tracked (mk-tracked-topology
                   cluster
                   (topology
                     {"1" (spout-spec feeder)}
                     {"2" (bolth-spec {"1" :shuffle} identity-bolth)
                      "3" (bolth-spec {"1" :shuffle} identity-bolth)
                      "4" (bolth-spec
                            {"2" :shuffle
                             "3" :shuffle}
                             (agg-bolth 4))}))]
      (submit-local-topology (:nimbus cluster)
                             "test-acking2"
                             {}
                             (:topology tracked))
      (.feed feeder [1])
      (tracked-wait tracked 1)
      (checker 0)
      (.feed feeder [1])
      (tracked-wait tracked 1)
      (checker 2)
      )))

(defbolth dup-anchor ["num"]
  [tuple collector]
  (emit-bolth! collector [1] :anchor [tuple tuple])
  (ack! collector tuple))

(deftest test-acking-self-anchor
  (with-tracked-cluster [cluster]
    (let [[feeder checker] (ack-tracking-feeder ["num"])
          tracked (mk-tracked-topology
                   cluster
                   (topology
                     {"1" (spout-spec feeder)}
                     {"2" (bolth-spec {"1" :shuffle} dup-anchor)
                      "3" (bolth-spec {"2" :shuffle} ack-bolth)}))]
      (submit-local-topology (:nimbus cluster)
                             "test"
                             {}
                             (:topology tracked))
      (.feed feeder [1])
      (tracked-wait tracked 1)
      (checker 1)
      (.feed feeder [1])
      (.feed feeder [1])
      (.feed feeder [1])
      (tracked-wait tracked 3)
      (checker 3)
      )))

;; (defspout ConstantSpout ["val"] {:prepare false}
;;   [collector]
;;   (Time/sleep 100)
;;   (emit-spout! collector [1]))

;; (def errored (atom false))
;; (def restarted (atom false))

;; (defbolth local-error-checker {} [tuple collector]
;;   (when-not @errored
;;     (reset! errored true)
;;     (println "erroring")
;;     (throw (RuntimeException.)))
;;   (when-not @restarted (println "restarted"))
;;   (reset! restarted true))

;; (deftest test-no-halt-local-mode
;;   (with-simulated-time-local-cluster [cluster]
;;       (let [topology (topology
;;                       {1 (spout-spec ConstantSpout)}
;;                       {2 (bolth-spec {1 :shuffle} local-error-checker)
;;                        })]
;;         (submit-local-topology (:nimbus cluster)
;;                                "test"
;;                                {}
;;                                topology)
;;         (while (not @restarted)
;;           (advance-time-ms! 100))
;;         )))

(defspout IncSpout ["word"]
  [conf context collector]
  (let [state (atom 0)]
    (spout
     (nextTuple []
       (Thread/sleep 100)
       (emit-spout! collector [@state] :id 1)         
       )
     (ack [id]
       (swap! state inc))
     )))


(defspout IncSpout2 ["word"] {:params [prefix]}
  [conf context collector]
  (let [state (atom 0)]
    (spout
     (nextTuple []
       (Thread/sleep 100)
       (swap! state inc)
       (emit-spout! collector [(str prefix "-" @state)])         
       )
     )))

;; (deftest test-clojure-spout
;;   (with-local-cluster [cluster]
;;     (let [nimbus (:nimbus cluster)
;;           top (topology
;;                {1 (spout-spec IncSpout)}
;;                {}
;;                )]
;;       (submit-local-topology nimbus
;;                              "spout-test"
;;                              {TOPOLOGY-DEBUG true
;;                               TOPOLOGY-MESSAGE-TIMEOUT-SECS 3}
;;                              top)
;;       (Thread/sleep 10000)
;;       (.killTopology nimbus "spout-test")
;;       (Thread/sleep 10000)
;;       )))


(deftest test-component-specific-config
  (with-simulated-time-local-cluster [cluster
                                      :daemon-conf {TOPOLOGY-OPTIMIZE false
                                                    TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS true}]
    (letlocals
     (bind builder (TopologyBuilder.))
     (.setSpout builder "1" (TestPlannerSpout. (Fields. ["conf"])))
     (-> builder
         (.setbolth "2"
                   (TestConfbolth.
                    {"fake.config" 123
                     TOPOLOGY-MAX-TASK-PARALLELISM 20
                     TOPOLOGY-MAX-SPOUT-PENDING 30
                     TOPOLOGY-OPTIMIZE true
                     TOPOLOGY-KRYO-REGISTER [{"fake.type" "bad.serializer"}
                                             {"fake.type2" "a.serializer"}]
                     }))
         (.shuffleGrouping "1")
         (.setMaxTaskParallelism (int 2))
         (.addConfiguration "fake.config2" 987)
         )
     

     (bind results
           (complete-topology cluster
                              (.createTopology builder)
                              :storm-conf {TOPOLOGY-KRYO-REGISTER [{"fake.type" "good.serializer" "fake.type3" "a.serializer3"}]}
                              :mock-sources {"1" [["fake.config"]
                                                  [TOPOLOGY-MAX-TASK-PARALLELISM]
                                                  [TOPOLOGY-MAX-SPOUT-PENDING]
                                                  [TOPOLOGY-OPTIMIZE]
                                                  ["fake.config2"]
                                                  [TOPOLOGY-KRYO-REGISTER]
                                                  ]}))
     (is (= {"fake.config" 123
             "fake.config2" 987
             TOPOLOGY-MAX-TASK-PARALLELISM 2
             TOPOLOGY-MAX-SPOUT-PENDING 30
             TOPOLOGY-OPTIMIZE false
             TOPOLOGY-KRYO-REGISTER {"fake.type" "good.serializer"
                                     "fake.type2" "a.serializer"
                                     "fake.type3" "a.serializer3"}}
            (->> (read-tuples results "2")
                 (apply concat)
                 (apply hash-map))
            ))
     )))

(defbolth hooks-bolth ["emit" "ack" "fail"] {:prepare true}
  [conf context collector]
  (let [acked (atom 0)
        failed (atom 0)
        emitted (atom 0)]
    (.addTaskHook context
                  (reify backtype.storm.hooks.ITaskHook
                    (prepare [this conf context]
                      )
                    (cleanup [this]
                      )
                    (emit [this info]
                      (swap! emitted inc))
                    (bolthAck [this info]
                      (swap! acked inc))
                    (bolthFail [this info]
                      (swap! failed inc))))
    (bolth
     (execute [tuple]
        (emit-bolth! collector [@emitted @acked @failed])
        (if (= 0 (- @acked @failed))
          (ack! collector tuple)
          (fail! collector tuple))
        ))))

(deftest test-hooks
  (with-simulated-time-local-cluster [cluster]
    (let [topology (topology {"1" (spout-spec (TestPlannerSpout. (Fields. ["conf"])))
                              }
                             {"2" (bolth-spec {"1" :shuffle}
                                             hooks-bolth)
                              })
          results (complete-topology cluster
                                     topology
                                     :mock-sources {"1" [[1]
                                                         [1]
                                                         [1]
                                                         [1]
                                                         ]})]
      (is (= [[0 0 0]
              [2 1 0]
              [4 1 1]
              [6 2 1]]
             (read-tuples results "2")
             )))))

(deftest test-acking-branching-complex
  ;; test acking with branching in the topology
  )


(deftest test-fields-grouping
  ;; 1. put a shitload of random tuples through it and test that counts are right
  ;; 2. test that different spouts with different phints group the same way
  )

(deftest test-all-grouping
  )

(deftest test-direct-grouping
  )
