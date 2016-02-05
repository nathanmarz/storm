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
(ns org.apache.storm.daemon.builtin-metrics
  (:import [org.apache.storm.metric.api CountMetric StateMetric IMetric IStatefulObject])
  (:import [org.apache.storm.metric.internal MultiCountStatAndMetric MultiLatencyStatAndMetric])
  (:import [org.apache.storm Config])
  (:use [org.apache.storm.stats]))

(defrecord BuiltinSpoutMetrics [^MultiCountStatAndMetric ack-count
                                ^MultiLatencyStatAndMetric complete-latency
                                ^MultiCountStatAndMetric fail-count
                                ^MultiCountStatAndMetric emit-count
                                ^MultiCountStatAndMetric transfer-count])
(defrecord BuiltinBoltMetrics [^MultiCountStatAndMetric ack-count
                               ^MultiLatencyStatAndMetric process-latency
                               ^MultiCountStatAndMetric fail-count
                               ^MultiCountStatAndMetric execute-count
                               ^MultiLatencyStatAndMetric execute-latency
                               ^MultiCountStatAndMetric emit-count
                               ^MultiCountStatAndMetric transfer-count])
(defrecord SpoutThrottlingMetrics [^CountMetric skipped-max-spout
                                   ^CountMetric skipped-throttle
                                   ^CountMetric skipped-inactive])


(defn make-data [executor-type stats]
  (condp = executor-type
    :spout (BuiltinSpoutMetrics. (stats-acked stats)
                                 (stats-complete-latencies stats)
                                 (stats-failed stats)
                                 (stats-emitted stats)
                                 (stats-transferred stats))
    :bolt (BuiltinBoltMetrics. (stats-acked stats)
                               (stats-process-latencies stats)
                               (stats-failed stats)
                               (stats-executed stats)
                               (stats-execute-latencies stats)
                               (stats-emitted stats)
                               (stats-transferred stats))))

(defn make-spout-throttling-data []
  (SpoutThrottlingMetrics. (CountMetric.)
                           (CountMetric.)
                           (CountMetric.)))

(defn register-spout-throttling-metrics [throttling-metrics  storm-conf topology-context]
  (doseq [[kw imetric] throttling-metrics]
    (.registerMetric topology-context (str "__" (name kw)) imetric
                     (int (get storm-conf Config/TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS)))))

(defn register-all [builtin-metrics  storm-conf topology-context]
  (doseq [[kw imetric] builtin-metrics]
    (.registerMetric topology-context (str "__" (name kw)) imetric
                     (int (get storm-conf Config/TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS)))))

(defn register-iconnection-server-metric [server storm-conf topology-context]
  (if (instance? IStatefulObject server)
    (.registerMetric topology-context "__recv-iconnection" (StateMetric. server)
                     (int (get storm-conf Config/TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS)))))

(defn register-iconnection-client-metrics [node+port->socket-ref storm-conf topology-context]
  (.registerMetric topology-context "__send-iconnection"
    (reify IMetric
      (^Object getValueAndReset [this]
        (into {}
          (map
            (fn [[node+port ^IStatefulObject connection]] [node+port (.getState connection)])
            (filter 
              (fn [[node+port connection]] (instance? IStatefulObject connection))
              @node+port->socket-ref)))))
    (int (get storm-conf Config/TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS))))
 
(defn register-queue-metrics [queues storm-conf topology-context]
  (doseq [[qname q] queues]
    (.registerMetric topology-context (str "__" (name qname)) (StateMetric. q)
                     (int (get storm-conf Config/TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS)))))

(defn skipped-max-spout! [^SpoutThrottlingMetrics m stats]
  (-> m .skipped-max-spout (.incrBy (stats-rate stats))))

(defn skipped-throttle! [^SpoutThrottlingMetrics m stats]
  (-> m .skipped-throttle (.incrBy (stats-rate stats))))

(defn skipped-inactive! [^SpoutThrottlingMetrics m stats]
  (-> m .skipped-inactive (.incrBy (stats-rate stats))))
