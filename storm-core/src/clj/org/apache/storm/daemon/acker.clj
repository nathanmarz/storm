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
(ns org.apache.storm.daemon.acker
  (:import [org.apache.storm.task OutputCollector TopologyContext IBolt]
           [org.apache.storm.utils Utils])
  (:import [org.apache.storm.tuple Tuple Fields])
  (:import [org.apache.storm.utils Container RotatingMap MutableObject])
  (:import [java.util List Map])
  (:import [org.apache.storm Constants]
           (org.apache.storm.daemon AckerBolt))
  (:use [org.apache.storm config log])
  (:gen-class
   :init init
   :implements [org.apache.storm.task.IBolt]
   :constructors {[] []}
   :state state))

(def ACKER-COMPONENT-ID AckerBolt/ACKER_COMPONENT_ID)
(def ACKER-INIT-STREAM-ID AckerBolt/ACKER_INIT_STREAM_ID)
(def ACKER-ACK-STREAM-ID AckerBolt/ACKER_ACK_STREAM_ID)
(def ACKER-FAIL-STREAM-ID AckerBolt/ACKER_FAIL_STREAM_ID)

(defn mk-acker-bolt []
  (let [output-collector (MutableObject.)
        pending (MutableObject.)]
    (AckerBolt.)))

(defn -init []
  [[] (Container.)])

(defn -prepare [^org.apache.storm.daemon.acker this conf context collector]
  (let [^IBolt ret (mk-acker-bolt)]
    (.. this state (set ret))
    (.prepare ret conf context collector)
    ))

(defn -execute [^org.apache.storm.daemon.acker this tuple]
  (let [^IBolt delegate (.. this state (get))]
    (.execute delegate tuple)
    ))

(defn -cleanup [^org.apache.storm.daemon.acker this]
  (let [^IBolt delegate (.. this state (get))]
    (.cleanup delegate)
    ))
