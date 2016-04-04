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
(ns org.apache.storm.starter.clj.rolling-top-words
  (:require [org.apache.storm [clojure :refer :all] [config :refer :all]]
            [org.apache.storm.starter.clj.bolts :refer
             [rolling-count-bolt intermediate-rankings-bolt total-rankings-bolt]])
  (:import [org.apache.storm StormSubmitter LocalCluster]
           [org.apache.storm.utils Utils]
           [org.apache.storm.testing TestWordSpout])
  (:gen-class))

(defn mk-topology []
  (let [spout-id "wordGenerator"
        counter-id "counter"
        ranker-id "intermediateRanker"
        total-ranker-id "finalRanker"]
    (topology
     {spout-id   (spout-spec (TestWordSpout.) :p 5)}
     {counter-id (bolt-spec {spout-id ["word"]}
                            (rolling-count-bolt 9 3)
                            :p 4)
      ranker-id (bolt-spec {counter-id ["obj"]}
                           (intermediate-rankings-bolt 5 2)
                           :p 4)
      total-ranker-id (bolt-spec {ranker-id :global}
                                 (total-rankings-bolt 5 2))})))

(defn run-local! []
  (let [cluster (LocalCluster.)]
    (.submitTopology cluster "slidingWindowCounts" {TOPOLOGY-DEBUG true} (mk-topology))
    (Utils/sleep 60000)
    (.shutdown cluster)))

(defn submit-topology! [name]
  (StormSubmitter/submitTopology
   name
   {TOPOLOGY-DEBUG true
    TOPOLOGY-WORKERS 3}
   (mk-topology)))

(defn -main
  ([]
   (run-local!))
  ([name]
   (submit-topology! name)))
