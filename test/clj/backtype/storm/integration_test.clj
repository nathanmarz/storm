(ns backtype.storm.integration-test
  (:use [clojure test])
  (:import [backtype.storm.testing TestWordCounter TestWordSpout TestGlobalCount TestAggregatesCounter])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  )

(bootstrap)

;; (deftest test-counter
;;   (with-local-cluster [cluster :supervisors 4]
;;     (let [state (:storm-cluster-state cluster)
;;           nimbus (:nimbus cluster)
;;           topology (thrift/mk-topology
;;                     {1 (thrift/mk-spout-spec (TestWordSpout. true) :parallelism-hint 3)}
;;                     {2 (thrift/mk-bolt-spec {1 ["word"]} (TestWordCounter.) :parallelism-hint 4)
;;                      3 (thrift/mk-bolt-spec {1 :global} (TestGlobalCount.))
;;                      4 (thrift/mk-bolt-spec {2 :global} (TestAggregatesCounter.))
;;                      })]
;;         (submit-local-topology nimbus
;;                             "counter"
;;                             {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 20 TOPOLOGY-MESSAGE-TIMEOUT-SECS 3 TOPOLOGY-DEBUG true}
;;                             topology)
;;         (Thread/sleep 10000)
;;         (.killTopology nimbus "counter")
;;         (Thread/sleep 10000)
;;         )))

;; (deftest test-multilang-fy
;;   (with-local-cluster [cluster :supervisors 4]
;;     (let [nimbus (:nimbus cluster)
;;           topology (thrift/mk-topology
;;                       {1 (thrift/mk-spout-spec (TestWordSpout. false))}
;;                       {2 (thrift/mk-shell-bolt-spec {1 :shuffle} "fancy" "tester.fy" ["word"] :parallelism-hint 1)}
;;                       )]
;;       (submit-local-topology nimbus
;;                           "test"
;;                           {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 20 TOPOLOGY-MESSAGE-TIMEOUT-SECS 3 TOPOLOGY-DEBUG true}
;;                           topology)
;;       (Thread/sleep 10000)
;;       (.killTopology nimbus "test")
;;       (Thread/sleep 10000)
;;       )))

;; (deftest test-multilang-rb
;;   (with-local-cluster [cluster :supervisors 4]
;;     (let [nimbus (:nimbus cluster)
;;           topology (thrift/mk-topology
;;                       {1 (thrift/mk-spout-spec (TestWordSpout. false))}
;;                       {2 (thrift/mk-shell-bolt-spec {1 :shuffle} "ruby" "tester.rb" ["word"] :parallelism-hint 1)}
;;                       )]
;;       (submit-local-topology nimbus
;;                           "test"
;;                           {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 20 TOPOLOGY-MESSAGE-TIMEOUT-SECS 3 TOPOLOGY-DEBUG true}
;;                           topology)
;;       (Thread/sleep 10000)
;;       (.killTopology nimbus "test")
;;       (Thread/sleep 10000)
;;       )))


(deftest test-multilang-py
  (with-local-cluster [cluster :supervisors 4]
    (let [nimbus (:nimbus cluster)
          topology (thrift/mk-topology
                      {1 (thrift/mk-spout-spec (TestWordSpout. false))}
                      {2 (thrift/mk-shell-bolt-spec {1 :shuffle} "python" "tester.py" ["word"] :parallelism-hint 1)}
                      )]
      (submit-local-topology nimbus
                          "test"
                          {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 20 TOPOLOGY-MESSAGE-TIMEOUT-SECS 3 TOPOLOGY-DEBUG true}
                          topology)
      (Thread/sleep 10000)
      (.killTopology nimbus "test")
      (Thread/sleep 10000)
      )))


(deftest test-basic-topology
  (doseq [zmq-on? [true false]]
    (with-simulated-time-local-cluster [cluster :supervisors 4
                                        :daemon-conf {STORM-LOCAL-MODE-ZMQ zmq-on?}]
      (let [topology (thrift/mk-topology
                      {1 (thrift/mk-spout-spec (TestWordSpout. true) :parallelism-hint 3)}
                      {2 (thrift/mk-bolt-spec {1 ["word"]} (TestWordCounter.) :parallelism-hint 4)
                       3 (thrift/mk-bolt-spec {1 :global} (TestGlobalCount.))
                       4 (thrift/mk-bolt-spec {2 :global} (TestAggregatesCounter.))
                       })
            results (complete-topology cluster
                                       topology
                                       :mock-sources {1 [["nathan"] ["bob"] ["joey"] ["nathan"]]}
                                       :storm-conf {TOPOLOGY-DEBUG true
                                                    TOPOLOGY-WORKERS 2})]
        (is (ms= [["nathan"] ["bob"] ["joey"] ["nathan"]]
                 (read-tuples results 1)))
        (is (ms= [["nathan" 1] ["nathan" 2] ["bob" 1] ["joey" 1]]
                 (read-tuples results 2)))
        (is (= [[1] [2] [3] [4]]
               (read-tuples results 3)))
        (is (= [[1] [2] [3] [4]]
               (read-tuples results 4)))
        ))))

(deftest test-shuffle
  (with-simulated-time-local-cluster [cluster :supervisors 4]
    (let [topology (thrift/mk-topology
                    {1 (thrift/mk-spout-spec (TestWordSpout. true) :parallelism-hint 4)}
                    {2 (thrift/mk-bolt-spec {1 :shuffle} (TestGlobalCount.)
                                            :parallelism-hint 6)
                     })
          results (complete-topology cluster
                                     topology
                                     ;; important for test that
                                     ;; #tuples = multiple of 4 and 6
                                     :mock-sources {1 [["a"] ["b"]
                                                       ["a"] ["b"]
                                                       ["a"] ["b"]
                                                       ["a"] ["b"]
                                                       ["a"] ["b"]
                                                       ["a"] ["b"]
                                                       ["a"] ["b"]
                                                       ["a"] ["b"]
                                                       ["a"] ["b"]
                                                       ["a"] ["b"]
                                                       ["a"] ["b"]
                                                       ["a"] ["b"]
                                                       ]}
                                     )]
      (is (ms= (apply concat (repeat 6 [[1] [2] [3] [4]]))
               (read-tuples results 2)))
      )))

(defbolt lalala-bolt1 ["word"] [tuple collector]
  (let [ret (-> (.getValue tuple 0) (str "lalala"))]
    (.emit collector tuple [ret])
    (.ack collector tuple)
    ))

(defbolt lalala-bolt2 ["word"] {:prepare true}
  [conf context collector]
  (let [state (atom nil)]
    (reset! state "lalala")
    (bolt
      (execute [tuple]
        (let [ret (-> (.getValue tuple 0) (str @state))]
                (.emit collector tuple [ret])
                (.ack collector tuple)
                ))
      )))
      
(defbolt lalala-bolt3 ["word"] {:prepare true :params [prefix]}
  [conf context collector]
  (let [state (atom nil)]
    (bolt
      (prepare [_ _ _]
        (reset! state (str prefix "lalala")))
      (execute [tuple]
        (let [ret (-> (.getValue tuple 0) (str @state))]
          (.emit collector tuple [ret])
          (.ack collector tuple)
          )))
    ))

(deftest test-clojure-bolt
  (with-simulated-time-local-cluster [cluster :supervisors 4]
    (let [nimbus (:nimbus cluster)
          topology (thrift/mk-topology
                      {1 (thrift/mk-spout-spec (TestWordSpout. false))}
                      {2 (thrift/mk-bolt-spec {1 :shuffle}
                                              lalala-bolt1)
                       3 (thrift/mk-bolt-spec {1 :shuffle}
                                              lalala-bolt2)
                       4 (thrift/mk-bolt-spec {1 :shuffle}
                                              (lalala-bolt3 "_nathan_"))}
                      )
          results (complete-topology cluster
                                     topology
                                     :mock-sources {1 [["david"]
                                                       ["adam"]
                                                       ]}
                                     )]
      (is (ms= [["davidlalala"] ["adamlalala"]] (read-tuples results 2)))
      (is (ms= [["davidlalala"] ["adamlalala"]] (read-tuples results 3)))
      (is (ms= [["david_nathan_lalala"] ["adam_nathan_lalala"]] (read-tuples results 4)))
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

(defbolt branching-bolt ["num"]
  {:params [amt]}
  [tuple collector]
  (doseq [i (range amt)]
    (emit-bolt! collector [i] :anchor tuple))
  (ack! collector tuple))

(defbolt agg-bolt ["num"] {:prepare true :params [amt]}
  [conf context collector]
  (let [seen (atom [])]
    (bolt
      (execute [tuple]
        (swap! seen conj tuple)
        (when (= (count @seen) amt)
          (emit-bolt! collector [1] :anchor @seen)
          (doseq [s @seen]
            (ack! collector s))
          (reset! seen [])
          )))
      ))

(defbolt ack-bolt {}
  [tuple collector]
  (ack! collector tuple))

(defbolt identity-bolt ["num"]
  [tuple collector]
  (emit-bolt! collector (.getValues tuple) :anchor tuple)
  (ack! collector tuple))

(deftest test-acking
  (with-tracked-cluster [cluster]
    (let [[feeder1 checker1] (ack-tracking-feeder ["num"])
          [feeder2 checker2] (ack-tracking-feeder ["num"])
          [feeder3 checker3] (ack-tracking-feeder ["num"])
          tracked (mk-tracked-topology
                   {1 [feeder1]
                    2 [feeder2]
                    3 [feeder3]}
                   {4 [{1 :shuffle} (branching-bolt 2)]
                    5 [{2 :shuffle} (branching-bolt 4)]
                    6 [{3 :shuffle} (branching-bolt 1)]
                    7 [{4 :shuffle
                        5 :shuffle
                        6 :shuffle} (agg-bolt 3)]
                    8 [{7 :shuffle} (branching-bolt 2)]
                    9 [{8 :shuffle} ack-bolt]}
                   )]
      (submit-local-topology (:nimbus cluster)
                             "test"
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
                   {1 [feeder]}
                   {2 [{1 :shuffle} identity-bolt]
                    3 [{1 :shuffle} identity-bolt]
                    4 [{2 :shuffle
                        3 :shuffle} (agg-bolt 4)]})]
      (submit-local-topology (:nimbus cluster)
                             "test"
                             {}
                             (:topology tracked))
      (.feed feeder [1])
      (tracked-wait tracked 1)
      (checker 0)
      (.feed feeder [1])
      (tracked-wait tracked 1)
      (checker 2)
      )))

(defbolt dup-anchor ["num"]
  [tuple collector]
  (emit-bolt! collector [1] :anchor [tuple tuple])
  (ack! collector tuple))

(deftest test-acking-self-anchor
  (with-tracked-cluster [cluster]
    (let [[feeder checker] (ack-tracking-feeder ["num"])
          tracked (mk-tracked-topology
                   {1 [feeder]}
                   {2 [{1 :shuffle} dup-anchor]
                    3 [{2 :shuffle} ack-bolt]})]
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

;; (defbolt local-error-checker {} [tuple collector]
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
;;                       {2 (bolt-spec {1 :shuffle} local-error-checker)
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
