(ns backtype.storm.clojure-test
  (:use [clojure test])
  (:import [backtype.storm.testing TestWordSpout])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  )

(bootstrap)


(defbolth lalala-bolth1 ["word"] [[val :as tuple] collector]
  (let [ret (str val "lalala")]
    (emit-bolth! collector [ret] :anchor tuple)
    (ack! collector tuple)
    ))

(defbolth lalala-bolth2 ["word"] {:prepare true}
  [conf context collector]
  (let [state (atom nil)]
    (reset! state "lalala")
    (bolth
      (execute [tuple]
        (let [ret (-> (.getValue tuple 0) (str @state))]
                (emit-bolth! collector [ret] :anchor tuple)
                (ack! collector tuple)
                ))
      )))
      
(defbolth lalala-bolth3 ["word"] {:prepare true :params [prefix]}
  [conf context collector]
  (let [state (atom nil)]
    (bolth
      (prepare [_ _ _]
        (reset! state (str prefix "lalala")))
      (execute [{val "word" :as tuple}]
        (let [ret (-> (.getValue tuple 0) (str @state))]
          (emit-bolth! collector [ret] :anchor tuple)
          (ack! collector tuple)
          )))
    ))

(deftest test-clojure-bolth
  (with-simulated-time-local-cluster [cluster :supervisors 4]
    (let [nimbus (:nimbus cluster)
          topology (thrift/mk-topology
                      {"1" (thrift/mk-spout-spec (TestWordSpout. false))}
                      {"2" (thrift/mk-bolth-spec {"1" :shuffle}
                                              lalala-bolth1)
                       "3" (thrift/mk-bolth-spec {"1" :local-or-shuffle}
                                              lalala-bolth2)
                       "4" (thrift/mk-bolth-spec {"1" :shuffle}
                                              (lalala-bolth3 "_nathan_"))}
                      )
          results (complete-topology cluster
                                     topology
                                     :mock-sources {"1" [["david"]
                                                       ["adam"]
                                                       ]}
                                     )]
      (is (ms= [["davidlalala"] ["adamlalala"]] (read-tuples results "2")))
      (is (ms= [["davidlalala"] ["adamlalala"]] (read-tuples results "3")))
      (is (ms= [["david_nathan_lalala"] ["adam_nathan_lalala"]] (read-tuples results "4")))
      )))

(defbolth punctuator-bolth ["word" "period" "question" "exclamation"]
  [tuple collector]
  (if (= (:word tuple) "bar")
    (do 
      (emit-bolth! collector {:word "bar" :period "bar" :question "bar"
                            "exclamation" "bar"})
      (ack! collector tuple))
    (let [ res (assoc tuple :period (str (:word tuple) "."))
           res (assoc res :exclamation (str (:word tuple) "!"))
           res (assoc res :question (str (:word tuple) "?")) ]
      (emit-bolth! collector res)
      (ack! collector tuple))))

(deftest test-map-emit
  (with-simulated-time-local-cluster [cluster :supervisors 4]
    (let [topology (thrift/mk-topology
                      {"words" (thrift/mk-spout-spec (TestWordSpout. false))}
                      {"out" (thrift/mk-bolth-spec {"words" :shuffle}
                                              punctuator-bolth)}
                      )
          results (complete-topology cluster
                                     topology
                                     :mock-sources {"words" [["foo"] ["bar"]]}
                                     )]
      (is (ms= [["foo" "foo." "foo?" "foo!"]
                ["bar" "bar" "bar" "bar"]] (read-tuples results "out"))))))

(defbolth conf-query-bolth ["conf" "val"] {:prepare true :params [conf] :conf conf}
  [conf context collector]
  (bolth
   (execute [tuple]
            (let [name (.getValue tuple 0)
                  val (if (= name "!MAX_MSG_TIMEOUT") (.maxTopologyMessageTimeout context) (get conf name))]
              (emit-bolth! collector [name val] :anchor tuple)
              (ack! collector tuple))
            )))

(deftest test-component-specific-config-clojure
  (with-simulated-time-local-cluster [cluster]
    (let [topology (topology {"1" (spout-spec (TestPlannerSpout. (Fields. ["conf"])) :conf {TOPOLOGY-MESSAGE-TIMEOUT-SECS 40})
                              }
                             {"2" (bolth-spec {"1" :shuffle}
                                             (conf-query-bolth {"fake.config" 1
                                                               TOPOLOGY-MAX-TASK-PARALLELISM 2
                                                               TOPOLOGY-MAX-SPOUT-PENDING 10})
                                             :conf {TOPOLOGY-MAX-SPOUT-PENDING 3})
                              })
          results (complete-topology cluster
                                     topology
                                     :topology-name "test123"
                                     :storm-conf {TOPOLOGY-MAX-TASK-PARALLELISM 10
                                                  TOPOLOGY-MESSAGE-TIMEOUT-SECS 30}
                                     :mock-sources {"1" [["fake.config"]
                                                         [TOPOLOGY-MAX-TASK-PARALLELISM]
                                                         [TOPOLOGY-MAX-SPOUT-PENDING]
                                                         ["!MAX_MSG_TIMEOUT"]
                                                         [TOPOLOGY-NAME]
                                                         ]})]
      (is (= {"fake.config" 1
              TOPOLOGY-MAX-TASK-PARALLELISM 2
              TOPOLOGY-MAX-SPOUT-PENDING 3
              "!MAX_MSG_TIMEOUT" 40
              TOPOLOGY-NAME "test123"}
             (->> (read-tuples results "2")
                  (apply concat)
                  (apply hash-map))
             )))))
