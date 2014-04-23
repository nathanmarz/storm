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
(ns backtype.storm.daemon.builtin-metrics
  (:import [backtype.storm.metric.api MultiCountMetric MultiReducedMetric MeanReducer StateMetric])
  (:import [backtype.storm Config])
  (:use [backtype.storm.stats :only [stats-rate]]))

(defrecord BuiltinSpoutMetrics [^MultiCountMetric ack-count                                
                                ^MultiReducedMetric complete-latency
                                ^MultiCountMetric fail-count
                                ^MultiCountMetric emit-count
                                ^MultiCountMetric transfer-count])
(defrecord BuiltinBoltMetrics [^MultiCountMetric ack-count
                               ^MultiReducedMetric process-latency
                               ^MultiCountMetric fail-count
                               ^MultiCountMetric execute-count
                               ^MultiReducedMetric execute-latency
                               ^MultiCountMetric emit-count
                               ^MultiCountMetric transfer-count])

(defn make-data [executor-type]
  (condp = executor-type
    :spout (BuiltinSpoutMetrics. (MultiCountMetric.)
                                 (MultiReducedMetric. (MeanReducer.))
                                 (MultiCountMetric.)
                                 (MultiCountMetric.)
                                 (MultiCountMetric.))
    :bolt (BuiltinBoltMetrics. (MultiCountMetric.)
                               (MultiReducedMetric. (MeanReducer.))
                               (MultiCountMetric.)
                               (MultiCountMetric.)
                               (MultiReducedMetric. (MeanReducer.))
                               (MultiCountMetric.)
                               (MultiCountMetric.))))

(defn register-all [builtin-metrics  storm-conf topology-context]
  (doseq [[kw imetric] builtin-metrics]
    (.registerMetric topology-context (str "__" (name kw)) imetric
                     (int (get storm-conf Config/TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS)))))
          
(defn register-queue-metrics [queues storm-conf topology-context]
  (doseq [[qname q] queues]
    (.registerMetric topology-context (str "__" (name qname)) (StateMetric. q)
                     (int (get storm-conf Config/TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS)))))

(defn spout-acked-tuple! [^BuiltinSpoutMetrics m stats stream latency-ms]  
  (-> m .ack-count (.scope stream) (.incrBy (stats-rate stats)))
  (-> m .complete-latency (.scope stream) (.update latency-ms)))

(defn spout-failed-tuple! [^BuiltinSpoutMetrics m stats stream]  
  (-> m .fail-count (.scope stream) (.incrBy (stats-rate stats))))

(defn bolt-execute-tuple! [^BuiltinBoltMetrics m stats comp-id stream latency-ms]
  (let [scope (str comp-id ":" stream)]    
    (-> m .execute-count (.scope scope) (.incrBy (stats-rate stats)))
    (-> m .execute-latency (.scope scope) (.update latency-ms))))

(defn bolt-acked-tuple! [^BuiltinBoltMetrics m stats comp-id stream latency-ms]
  (let [scope (str comp-id ":" stream)]
    (-> m .ack-count (.scope scope) (.incrBy (stats-rate stats)))
    (-> m .process-latency (.scope scope) (.update latency-ms))))

(defn bolt-failed-tuple! [^BuiltinBoltMetrics m stats comp-id stream]
  (let [scope (str comp-id ":" stream)]    
    (-> m .fail-count (.scope scope) (.incrBy (stats-rate stats)))))

(defn emitted-tuple! [m stats stream]
  (-> m :emit-count (.scope stream) (.incrBy (stats-rate stats))))

(defn transferred-tuple! [m stats stream num-out-tasks]
  (-> m :transfer-count (.scope stream) (.incrBy (* num-out-tasks (stats-rate stats)))))
