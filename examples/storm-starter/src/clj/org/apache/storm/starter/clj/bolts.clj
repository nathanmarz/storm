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
(ns org.apache.storm.starter.clj.bolts
  (:require [org.apache.storm
             [clojure :refer :all]
             [config :refer :all]
             [log :refer :all]])
  (:import [org.apache.storm.starter.tools
            NthLastModifiedTimeTracker SlidingWindowCounter
            Rankings RankableObjectWithFields]
           [org.apache.storm.utils TupleUtils]))

(defbolt rolling-count-bolt ["obj" "count" "actualWindowLengthInSeconds"]
  {:prepare true
   :params [window-length emit-frequency]
   :conf {TOPOLOGY-TICK-TUPLE-FREQ-SECS emit-frequency}}
  [conf context collector]
  (let [num-windows (/ window-length emit-frequency)
        counter (SlidingWindowCounter. num-windows)
        tracker (NthLastModifiedTimeTracker. num-windows)]
    (bolt
     (execute [{word "word" :as tuple}]
       (if (TupleUtils/isTick tuple)
         (let [counts (.getCountsThenAdvanceWindow counter)
               actual-window-length (.secondsSinceOldestModification tracker)]
           (log-debug "Received tick tuple, triggering emit of current window counts")
           (.markAsModified tracker)
           (doseq [[obj count] counts]
             (emit-bolt! collector [obj count actual-window-length])))
         (do
           (.incrementCount counter word)
           (ack! collector tuple)))))))

(defmacro update-rankings [tuple collector rankings & body]
  `(if (TupleUtils/isTick ~tuple)
     (do
       (log-debug "Received tick tuple, triggering emit of current rankings")
       (emit-bolt! ~collector [(.copy ~rankings)])
       (log-debug "Rankings: " ~rankings))
     ~@body))

(defbolt intermediate-rankings-bolt ["rankings"]
  {:prepare true
   :params [top-n emit-frequency]
   :conf {TOPOLOGY-TICK-TUPLE-FREQ-SECS emit-frequency}}
  [conf context collector]
  (let [rankings (Rankings. top-n)]
    (bolt
     (execute [tuple]
       (update-rankings
        tuple collector rankings
        (.updateWith rankings (RankableObjectWithFields/from tuple)))))))

(defbolt total-rankings-bolt ["rankings"]
  {:prepare true
   :params [top-n emit-frequency]
   :conf {TOPOLOGY-TICK-TUPLE-FREQ-SECS emit-frequency}}
  [conf context collector]
  (let [rankings (Rankings. top-n)]
    (bolt
     (execute [{rankings-to-merge "rankings" :as tuple}]
       (update-rankings
        tuple collector rankings
        (doto rankings
          (.updateWith rankings-to-merge)
          (.pruneZeroCounts)))))))
