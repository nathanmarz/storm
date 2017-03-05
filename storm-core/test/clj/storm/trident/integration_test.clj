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
(ns storm.trident.integration-test
  (:use [clojure test])
  (:require [backtype.storm [testing :as t]])
  (:import [storm.trident.testing Split CountAsAggregator StringLength TrueFilter
            MemoryMapState$Factory])
  (:import [storm.trident.state StateSpec])
  (:import [storm.trident.operation.impl CombinerAggStateUpdater])
  (:use [storm.trident testing])
  (:use [backtype.storm util]))
  
(bootstrap-imports)

(deftest test-memory-map-get-tuples
  (t/with-local-cluster [cluster]
    (with-drpc [drpc]
      (letlocals
        (bind topo (TridentTopology.))
        (bind feeder (feeder-spout ["sentence"]))
        (bind word-counts
          (-> topo
              (.newStream "tester" feeder)
              (.each (fields "sentence") (Split.) (fields "word"))
              (.groupBy (fields "word"))
              (.persistentAggregate (memory-map-state) (Count.) (fields "count"))
              (.parallelismHint 6)
              ))       
        (-> topo
            (.newDRPCStream "all-tuples" drpc)
            (.broadcast)
            (.stateQuery word-counts (fields "args") (TupleCollectionGet.) (fields "word" "count"))
            (.project (fields "word" "count")))
        (with-topology [cluster topo]
          (feed feeder [["hello the man said"] ["the"]])
          (is (= #{["hello" 1] ["said" 1] ["the" 2] ["man" 1]}
                 (into #{} (exec-drpc drpc "all-tuples" "man"))))
          (feed feeder [["the foo"]])
          (is (= #{["hello" 1] ["said" 1] ["the" 3] ["man" 1] ["foo" 1]}
                 (into #{} (exec-drpc drpc "all-tuples" "man")))))))))

(deftest test-word-count
  (t/with-local-cluster [cluster]
    (with-drpc [drpc]
      (letlocals
        (bind topo (TridentTopology.))
        (bind feeder (feeder-spout ["sentence"]))
        (bind word-counts
          (-> topo
              (.newStream "tester" feeder)
              (.each (fields "sentence") (Split.) (fields "word"))
              (.groupBy (fields "word"))
              (.persistentAggregate (memory-map-state) (Count.) (fields "count"))
              (.parallelismHint 6)
              ))
        (-> topo
            (.newDRPCStream "words" drpc)
            (.each (fields "args") (Split.) (fields "word"))
            (.groupBy (fields "word"))
            (.stateQuery word-counts (fields "word") (MapGet.) (fields "count"))
            (.aggregate (fields "count") (Sum.) (fields "sum"))
            (.project (fields "sum")))
        (with-topology [cluster topo]
          (feed feeder [["hello the man said"] ["the"]])
          (is (= [[2]] (exec-drpc drpc "words" "the")))
          (is (= [[1]] (exec-drpc drpc "words" "hello")))
          (feed feeder [["the man on the moon"] ["where are you"]])
          (is (= [[4]] (exec-drpc drpc "words" "the")))
          (is (= [[2]] (exec-drpc drpc "words" "man")))
          (is (= [[8]] (exec-drpc drpc "words" "man where you the")))
          )))))

;; this test reproduces a bug where committer spouts freeze processing when 
;; there's at least one repartitioning after the spout
(deftest test-word-count-committer-spout
  (t/with-local-cluster [cluster]
    (with-drpc [drpc]
      (letlocals
        (bind topo (TridentTopology.))
        (bind feeder (feeder-committer-spout ["sentence"]))
        (.setWaitToEmit feeder false) ;;this causes lots of empty batches
        (bind word-counts
          (-> topo
              (.newStream "tester" feeder)
              (.parallelismHint 2)
              (.each (fields "sentence") (Split.) (fields "word"))
              (.groupBy (fields "word"))
              (.persistentAggregate (memory-map-state) (Count.) (fields "count"))
              (.parallelismHint 6)
              ))
        (-> topo
            (.newDRPCStream "words" drpc)
            (.each (fields "args") (Split.) (fields "word"))
            (.groupBy (fields "word"))
            (.stateQuery word-counts (fields "word") (MapGet.) (fields "count"))
            (.aggregate (fields "count") (Sum.) (fields "sum"))
            (.project (fields "sum")))
        (with-topology [cluster topo]
          (feed feeder [["hello the man said"] ["the"]])
          (is (= [[2]] (exec-drpc drpc "words" "the")))
          (is (= [[1]] (exec-drpc drpc "words" "hello")))
          (Thread/sleep 1000) ;; this is necessary to reproduce the bug where committer spouts freeze processing
          (feed feeder [["the man on the moon"] ["where are you"]])
          (is (= [[4]] (exec-drpc drpc "words" "the")))
          (is (= [[2]] (exec-drpc drpc "words" "man")))
          (is (= [[8]] (exec-drpc drpc "words" "man where you the")))
          (feed feeder [["the the"]])
          (is (= [[6]] (exec-drpc drpc "words" "the")))
          (feed feeder [["the"]])
          (is (= [[7]] (exec-drpc drpc "words" "the")))
          )))))


(deftest test-count-agg
  (t/with-local-cluster [cluster]
    (with-drpc [drpc]
      (letlocals
        (bind topo (TridentTopology.))
        (-> topo
            (.newDRPCStream "numwords" drpc)
            (.each (fields "args") (Split.) (fields "word"))
            (.aggregate (CountAsAggregator.) (fields "count"))
            (.parallelismHint 2) ;;this makes sure batchGlobal is working correctly
            (.project (fields "count")))
        (with-topology [cluster topo]
          (doseq [i (range 100)]
            (is (= [[1]] (exec-drpc drpc "numwords" "the"))))
          (is (= [[0]] (exec-drpc drpc "numwords" "")))
          (is (= [[8]] (exec-drpc drpc "numwords" "1 2 3 4 5 6 7 8")))
          )))))
          
(deftest test-split-merge
  (t/with-local-cluster [cluster]
    (with-drpc [drpc]
      (letlocals
        (bind topo (TridentTopology.))
        (bind drpc-stream (-> topo (.newDRPCStream "splitter" drpc)))
        (bind s1
          (-> drpc-stream
              (.each (fields "args") (Split.) (fields "word"))
              (.project (fields "word"))))
        (bind s2
          (-> drpc-stream
              (.each (fields "args") (StringLength.) (fields "len"))
              (.project (fields "len"))))

        (.merge topo [s1 s2])
        (with-topology [cluster topo]
          (is (t/ms= [[7] ["the"] ["man"]] (exec-drpc drpc "splitter" "the man")))
          (is (t/ms= [[5] ["hello"]] (exec-drpc drpc "splitter" "hello")))
          )))))

(deftest test-multiple-groupings-same-stream
  (t/with-local-cluster [cluster]
    (with-drpc [drpc]
      (letlocals
        (bind topo (TridentTopology.))
        (bind drpc-stream (-> topo (.newDRPCStream "tester" drpc)
                                   (.each (fields "args") (TrueFilter.))))
        (bind s1
          (-> drpc-stream
              (.groupBy (fields "args"))
              (.aggregate (CountAsAggregator.) (fields "count"))))
        (bind s2
          (-> drpc-stream
              (.groupBy (fields "args"))
              (.aggregate (CountAsAggregator.) (fields "count"))))

        (.merge topo [s1 s2])
        (with-topology [cluster topo]
          (is (t/ms= [["the" 1] ["the" 1]] (exec-drpc drpc "tester" "the")))
          (is (t/ms= [["aaaaa" 1] ["aaaaa" 1]] (exec-drpc drpc "tester" "aaaaa")))
          )))))
          
(deftest test-multi-repartition
  (t/with-local-cluster [cluster]
    (with-drpc [drpc]
      (letlocals
        (bind topo (TridentTopology.))
        (bind drpc-stream (-> topo (.newDRPCStream "tester" drpc)
                                   (.each (fields "args") (Split.) (fields "word"))
                                   (.shuffle)
                                   (.shuffle)
                                   (.aggregate (CountAsAggregator.) (fields "count"))
                                   ))
        (with-topology [cluster topo]
          (is (t/ms= [[2]] (exec-drpc drpc "tester" "the man")))
          (is (t/ms= [[1]] (exec-drpc drpc "tester" "aaa")))
          )))))

(deftest test-stream-projection-validation
  (t/with-local-cluster [cluster]
    (letlocals
     (bind feeder (feeder-committer-spout ["sentence"]))
     (bind topo (TridentTopology.))
     ;; valid projection fields will not throw exceptions
     (bind word-counts
           (-> topo
               (.newStream "tester" feeder)
               (.each (fields "sentence") (Split.) (fields "word"))
               (.groupBy (fields "word"))
               (.persistentAggregate (memory-map-state) (Count.) (fields "count"))
               (.parallelismHint 6)
               ))
     (bind stream (-> topo
                      (.newStream "tester" feeder)))
     ;; test .each
     (is (thrown? IllegalArgumentException
                  (-> stream
                      (.each (fields "sentence1") (Split.) (fields "word")))))
     ;; test .groupBy
     (is (thrown? IllegalArgumentException
                  (-> stream
                      (.each (fields "sentence") (Split.) (fields "word"))
                      (.groupBy (fields "word1")))))
     ;; test .aggregate
     (is (thrown? IllegalArgumentException
                  (-> stream
                      (.each (fields "sentence") (Split.) (fields "word"))
                      (.groupBy (fields "word"))
                      (.aggregate (fields "word1") (Count.) (fields "count")))))
     ;; test .project
     (is (thrown? IllegalArgumentException
                  (-> stream
                      (.project (fields "sentence1")))))
     ;; test .partitionBy
     (is (thrown? IllegalArgumentException
                  (-> stream
                      (.partitionBy (fields "sentence1")))))
     ;; test .partitionAggregate
     (is (thrown? IllegalArgumentException
                  (-> stream
                      (.each (fields "sentence") (Split.) (fields "word"))
                      (.partitionAggregate (fields "word1") (Count.) (fields "count")))))
     ;; test .persistentAggregate
     (is (thrown? IllegalArgumentException
                  (-> stream
                      (.each (fields "sentence") (Split.) (fields "word"))
                      (.groupBy (fields "word"))
                      (.persistentAggregate (StateSpec. (MemoryMapState$Factory.)) (fields "non-existent") (Count.) (fields "count")))))
     ;; test .partitionPersist
     (is (thrown? IllegalArgumentException
                  (-> stream
                      (.each (fields "sentence") (Split.) (fields "word"))
                      (.groupBy (fields "word"))
                      (.partitionPersist (StateSpec. (MemoryMapState$Factory.))
                                         (fields "non-existent")
                                         (CombinerAggStateUpdater. (Count.))
                                         (fields "count")))))
     ;; test .stateQuery
     (with-drpc [drpc]
       (is (thrown? IllegalArgumentException
                    (-> topo
                        (.newDRPCStream "words" drpc)
                        (.each (fields "args") (Split.) (fields "word"))
                        (.groupBy (fields "word"))
                        (.stateQuery word-counts (fields "word1") (MapGet.) (fields "count"))))))
     )))

;; (deftest test-split-merge
;;   (t/with-local-cluster [cluster]
;;     (with-drpc [drpc]
;;       (letlocals
;;         (bind topo (TridentTopology.))
;;         (bind drpc-stream (-> topo (.newDRPCStream "splitter" drpc)))
;;         (bind s1
;;           (-> drpc-stream
;;               (.each (fields "args") (Split.) (fields "word"))
;;               (.project (fields "word"))))
;;         (bind s2
;;           (-> drpc-stream
;;               (.each (fields "args") (StringLength.) (fields "len"))
;;               (.project (fields "len"))))
;; 
;;         (.merge topo [s1 s2])
;;         (with-topology [cluster topo]
;;           (is (t/ms= [[7] ["the"] ["man"]] (exec-drpc drpc "splitter" "the man")))
;;           (is (t/ms= [[5] ["hello"]] (exec-drpc drpc "splitter" "hello")))
;;           )))))
