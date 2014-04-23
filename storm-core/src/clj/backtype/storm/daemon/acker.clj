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
(ns backtype.storm.daemon.acker
  (:import [backtype.storm.task OutputCollector TopologyContext IBolt])
  (:import [backtype.storm.tuple Tuple Fields])
  (:import [backtype.storm.utils RotatingMap MutableObject])
  (:import [java.util List Map])
  (:import [backtype.storm Constants])
  (:use [backtype.storm config util log])
  (:gen-class
   :init init
   :implements [backtype.storm.task.IBolt]
   :constructors {[] []}
   :state state ))

(def ACKER-COMPONENT-ID "__acker")
(def ACKER-INIT-STREAM-ID "__ack_init")
(def ACKER-ACK-STREAM-ID "__ack_ack")
(def ACKER-FAIL-STREAM-ID "__ack_fail")

(defn- update-ack [curr-entry val]
  (let [old (get curr-entry :val 0)]
    (assoc curr-entry :val (bit-xor old val))
    ))

(defn- acker-emit-direct [^OutputCollector collector ^Integer task ^String stream ^List values]
  (.emitDirect collector task stream values)
  )

(defn mk-acker-bolt []
  (let [output-collector (MutableObject.)
        pending (MutableObject.)]
    (reify IBolt
      (^void prepare [this ^Map storm-conf ^TopologyContext context ^OutputCollector collector]
               (.setObject output-collector collector)
               (.setObject pending (RotatingMap. 2))
               )
      (^void execute [this ^Tuple tuple]
             (let [^RotatingMap pending (.getObject pending)
                   stream-id (.getSourceStreamId tuple)]
               (if (= stream-id Constants/SYSTEM_TICK_STREAM_ID)
                 (.rotate pending)
                 (let [id (.getValue tuple 0)
                       ^OutputCollector output-collector (.getObject output-collector)
                       curr (.get pending id)
                       curr (condp = stream-id
                                ACKER-INIT-STREAM-ID (-> curr
                                                         (update-ack (.getValue tuple 1))
                                                         (assoc :spout-task (.getValue tuple 2)))
                                ACKER-ACK-STREAM-ID (update-ack curr (.getValue tuple 1))
                                ACKER-FAIL-STREAM-ID (assoc curr :failed true))]
                   (.put pending id curr)
                   (when (and curr (:spout-task curr))
                     (cond (= 0 (:val curr))
                           (do
                             (.remove pending id)
                             (acker-emit-direct output-collector
                                                (:spout-task curr)
                                                ACKER-ACK-STREAM-ID
                                                [id]
                                                ))
                           (:failed curr)
                           (do
                             (.remove pending id)
                             (acker-emit-direct output-collector
                                                (:spout-task curr)
                                                ACKER-FAIL-STREAM-ID
                                                [id]
                                                ))
                           ))
                   (.ack output-collector tuple)
                   ))))
      (^void cleanup [this]
        )
      )))

(defn -init []
  [[] (container)])

(defn -prepare [this conf context collector]
  (let [^IBolt ret (mk-acker-bolt)]
    (container-set! (.state ^backtype.storm.daemon.acker this) ret)
    (.prepare ret conf context collector)
    ))

(defn -execute [this tuple]
  (let [^IBolt delegate (container-get (.state ^backtype.storm.daemon.acker this))]
    (.execute delegate tuple)
    ))

(defn -cleanup [this]
  (let [^IBolt delegate (container-get (.state ^backtype.storm.daemon.acker this))]
    (.cleanup delegate)
    ))
