(ns storm.trident.component-name-test
  (:use [clojure test])
  (:require [backtype.storm [testing :as t]])
  (:import [storm.trident.testing Split])
  (:use [storm.trident testing])
  (:use [backtype.storm util]))
  
(bootstrap-imports)

(deftest test-component-name
  (letlocals
   (bind topo (TridentTopology.))
   (bind feeder (feeder-spout ["sentence"]))
   
   ;; test the name without manually specifing stream name
   (-> topo
       (.newStream "tester" feeder)
       (.each (fields "sentence") (Split.) (fields "word")))
   (bind topo (.build topo))

   (bind bolt-names (-> topo .get_bolts .keySet))
   ;; filter out the coord bolts
   (bind bolt-names (remove #(.startsWith % "$spoutcoord-") bolt-names))
   (is (= #{"spout1" "bolt1"} (set bolt-names)))
   
   ;; test manually specifing stream name
   (bind topo (TridentTopology.))      
   (-> topo
       (.newStream "tester" feeder)
       (.name "feeder-spout")
       (.each (fields "sentence") (Split.) (fields "word"))
       (.name "split-word1")
       (.each (fields "sentence") (Split.) (fields "word1"))
       (.name "split-word2"))
   (bind topo (.build topo))
   (bind bolt-names (-> topo .get_bolts .keySet))
   ;; filter out the coord bolts
   (bind bolt-names (remove #(.startsWith % "$spoutcoord-") bolt-names))
   (is (= #{"feeder-spout" "split-word2"} (set bolt-names)))))
