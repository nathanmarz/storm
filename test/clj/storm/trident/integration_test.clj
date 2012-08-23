(ns storm.trident.integration-test
  (:use [clojure test])
  (:require [backtype.storm [testing :as t]])
  (:import [storm.trident.testing Split CountAsAggregator StringLength TrueFilter])
  (:use [storm.trident testing])
  (:use [backtype.storm util]))
  
(bootstrap-imports)

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
