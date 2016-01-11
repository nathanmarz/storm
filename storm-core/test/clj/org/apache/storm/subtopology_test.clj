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
(ns org.apache.storm.subtopology-test
  (:use [clojure test])
  (:import [org.apache.storm.topology TopologyBuilder])
  (:import [org.apache.storm.testing TestWordSpout PrepareBatchBolt BatchRepeatA BatchProcessWord BatchNumberList])
  (:import [org.apache.storm.coordination BatchSubtopologyBuilder])
  (:use [org.apache.storm testing])
  (:use [org.apache.storm.daemon common]))

;; todo: need to configure coordinatedbolt with streams that aren't subscribed to, should auto-anchor those to the final
;; coordination tuple... find all streams that aren't subscribed to
;; having trouble with this test, commenting for now
;; (deftest test-batch-subtopology
;;   (with-local-cluster [cluster :supervisors 4]
;;     (letlocals
;;       (bind builder (TopologyBuilder.))
;;       (.setSpout builder "spout" (TestWordSpout.))
;;       (-> (.setBolt builder "identity" (PrepareBatchBolt. (Fields. ["id" "word"])) 3)
;;           (.shuffleGrouping "spout")
;;           )
;;       (bind batch-builder (BatchSubtopologyBuilder. "for-a" (BatchRepeatA.) 2))
;;       (-> (.getMasterDeclarer batch-builder)
;;           (.shuffleGrouping "identity"))  
;;       (-> (.setBolt batch-builder "process" (BatchProcessWord.) 2)
;;           (.fieldsGrouping "for-a" "multi" (Fields. ["id"])))
;;       (-> (.setBolt batch-builder "joiner" (BatchNumberList. "for-a") 2)
;;           (.fieldsGrouping "process" (Fields. ["id"]))
;;           (.fieldsGrouping "for-a" "single" (Fields. ["id"]))
;;           )
;;       
;;       (.extendTopology batch-builder builder)
;;       
;;       (bind results (complete-topology cluster
;;                                        (.createTopology builder)
;;                                        :storm-conf {TOPOLOGY-DEBUG true}
;;                                        :mock-sources {"spout" [
;;                                        ["ccacccaa"]
;;                                        ["bbb"]
;;                                        ["ba"]
;;                                        ]}
;;                                        ))
;;       (is (ms= [
;;                 ["ccacccaa" [2 6 7]]
;;                 ["bbb" []]
;;                 ["ba" [1]]
;;                 ]
;;                (read-tuples results "joiner")))
;;       )))
