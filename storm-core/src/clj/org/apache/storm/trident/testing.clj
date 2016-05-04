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
(ns org.apache.storm.trident.testing
  (:import [org.apache.storm.trident.testing FeederBatchSpout FeederCommitterBatchSpout MemoryMapState MemoryMapState$Factory TuplifyArgs])
  (:import [org.apache.storm LocalDRPC])
  (:import [org.apache.storm.tuple Fields])
  (:import [org.apache.storm.generated KillOptions]
           [org.json.simple JSONValue])
  (:require [org.apache.storm [testing :as t]])
  (:use [org.apache.storm util])
  )

(defn local-drpc []
  (LocalDRPC.))

(defn exec-drpc [^LocalDRPC drpc function-name args]
  (if-let [res (.execute drpc function-name args)]
    (clojurify-structure (JSONValue/parse res))))

(defn exec-drpc-tuples [^LocalDRPC drpc function-name tuples]
  (exec-drpc drpc function-name (JSONValue/toJSONString tuples)))

(defn feeder-spout [fields]
  (FeederBatchSpout. fields))

(defn feeder-committer-spout [fields]
  (FeederCommitterBatchSpout. fields))

(defn feed [feeder tuples]
  (.feed feeder tuples))

(defn fields [& fields]
  (Fields. fields))

(defn memory-map-state []
  (MemoryMapState$Factory.))

(defmacro with-drpc [[drpc] & body]
  `(let [~drpc (org.apache.storm.LocalDRPC.)]
     ~@body
     (.shutdown ~drpc)
     ))

(defn with-topology* [cluster storm-topo body-fn]
  (t/submit-local-topology (:nimbus cluster) "tester" {} storm-topo)
  (body-fn)
  (.killTopologyWithOpts (:nimbus cluster) "tester" (doto (KillOptions.) (.set_wait_secs 0))))

(defmacro with-topology [[cluster topo storm-topo] & body]
  `(let [~storm-topo (.build ~topo)]
     (with-topology* ~cluster ~storm-topo (fn [] ~@body))))

(defn bootstrap-imports []
  (import 'org.apache.storm.LocalDRPC)
  (import 'org.apache.storm.trident.TridentTopology)
  (import '[org.apache.storm.trident.operation.builtin Count Sum Equals MapGet Debug FilterNull FirstN TupleCollectionGet])
  )

(defn drpc-tuples-input [topology function-name drpc outfields]
  (-> topology
      (.newDRPCStream function-name drpc)
      (.each (fields "args") (TuplifyArgs.) outfields)
      ))


