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
(ns org.apache.storm.starter.clj.exclamation
  (:import [org.apache.storm StormSubmitter LocalCluster]
           [org.apache.storm.utils Utils]
           [org.apache.storm.testing TestWordSpout])
  (:use [org.apache.storm clojure config])
  (:gen-class))

(defbolt exclamation-bolt ["word"]
  [{word "word" :as tuple} collector]
  (emit-bolt! collector [(str word "!!!")] :anchor tuple)
  (ack! collector tuple))

(defn mk-topology []
  (topology
   {"word"     (spout-spec (TestWordSpout.) :p 10)}
   {"exclaim1" (bolt-spec {"word" :shuffle} exclamation-bolt :p 3)
    "exclaim2" (bolt-spec {"exclaim1" :shuffle} exclamation-bolt :p 2)}))

(defn run-local! []
  (let [cluster (LocalCluster.)]
    (.submitTopology cluster "exclamation" {TOPOLOGY-DEBUG true} (mk-topology))
    (Utils/sleep 10000)
    (.killTopology cluster "exclamation")
    (.shutdown cluster)))

(defn submit-topology! [name]
  (StormSubmitter/submitTopologyWithProgressBar
   name
   {TOPOLOGY-DEBUG true
    TOPOLOGY-WORKERS 3}
   (mk-topology)))

(defn -main
  ([]
   (run-local!))
  ([name]
   (submit-topology! name)))
