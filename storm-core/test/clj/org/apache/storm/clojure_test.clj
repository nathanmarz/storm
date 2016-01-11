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
(ns org.apache.storm.clojure-test
  (:use [clojure test])
  (:import [org.apache.storm.testing TestWordSpout TestPlannerSpout]
           [org.apache.storm.tuple Fields])
  (:use [org.apache.storm testing clojure config])
  (:use [org.apache.storm.daemon common])
  (:require [org.apache.storm [thrift :as thrift]]))


(defbolt lalala-bolt1 ["word"] [[val :as tuple] collector]
  (let [ret (str val "lalala")]
    (emit-bolt! collector [ret] :anchor tuple)
    (ack! collector tuple)
    ))

(defbolt lalala-bolt2 ["word"] {:prepare true}
  [conf context collector]
  (let [state (atom nil)]
    (reset! state "lalala")
    (bolt
      (execute [tuple]
        (let [ret (-> (.getValue tuple 0) (str @state))]
                (emit-bolt! collector [ret] :anchor tuple)
                (ack! collector tuple)
                ))
      )))
      
(defbolt lalala-bolt3 ["word"] {:prepare true :params [prefix]}
  [conf context collector]
  (let [state (atom nil)]
    (bolt
      (prepare [_ _ _]
        (reset! state (str prefix "lalala")))
      (execute [{val "word" :as tuple}]
        (let [ret (-> (.getValue tuple 0) (str @state))]
          (emit-bolt! collector [ret] :anchor tuple)
          (ack! collector tuple)
          )))
    ))

(deftest test-clojure-bolt
  (with-simulated-time-local-cluster [cluster :supervisors 4]
    (let [nimbus (:nimbus cluster)
          topology (thrift/mk-topology
                      {"1" (thrift/mk-spout-spec (TestWordSpout. false))}
                      {"2" (thrift/mk-bolt-spec {"1" :shuffle}
                                              lalala-bolt1)
                       "3" (thrift/mk-bolt-spec {"1" :local-or-shuffle}
                                              lalala-bolt2)
                       "4" (thrift/mk-bolt-spec {"1" :shuffle}
                                              (lalala-bolt3 "_nathan_"))}
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

(defbolt punctuator-bolt ["word" "period" "question" "exclamation"]
  [tuple collector]
  (if (= (:word tuple) "bar")
    (do 
      (emit-bolt! collector {:word "bar" :period "bar" :question "bar"
                            "exclamation" "bar"})
      (ack! collector tuple))
    (let [ res (assoc tuple :period (str (:word tuple) "."))
           res (assoc res :exclamation (str (:word tuple) "!"))
           res (assoc res :question (str (:word tuple) "?")) ]
      (emit-bolt! collector res)
      (ack! collector tuple))))

(deftest test-map-emit
  (with-simulated-time-local-cluster [cluster :supervisors 4]
    (let [topology (thrift/mk-topology
                      {"words" (thrift/mk-spout-spec (TestWordSpout. false))}
                      {"out" (thrift/mk-bolt-spec {"words" :shuffle}
                                              punctuator-bolt)}
                      )
          results (complete-topology cluster
                                     topology
                                     :mock-sources {"words" [["foo"] ["bar"]]}
                                     )]
      (is (ms= [["foo" "foo." "foo?" "foo!"]
                ["bar" "bar" "bar" "bar"]] (read-tuples results "out"))))))

(defbolt conf-query-bolt ["conf" "val"] {:prepare true :params [conf] :conf conf}
  [conf context collector]
  (bolt
   (execute [tuple]
            (let [name (.getValue tuple 0)
                  val (if (= name "!MAX_MSG_TIMEOUT") (.maxTopologyMessageTimeout context) (get conf name))]
              (emit-bolt! collector [name val] :anchor tuple)
              (ack! collector tuple))
            )))

(deftest test-component-specific-config-clojure
  (with-simulated-time-local-cluster [cluster]
    (let [topology (topology {"1" (spout-spec (TestPlannerSpout. (Fields. ["conf"])) :conf {TOPOLOGY-MESSAGE-TIMEOUT-SECS 40})
                              }
                             {"2" (bolt-spec {"1" :shuffle}
                                             (conf-query-bolt {"fake.config" 1
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
