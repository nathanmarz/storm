(ns storm.trident.integration-test
  (:use [clojure test])
  (:require [backtype.storm [testing :as t]])
  (:import [storm.trident.testing Split])
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
