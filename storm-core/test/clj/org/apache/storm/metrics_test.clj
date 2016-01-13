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
(ns org.apache.storm.metrics-test
  (:use [clojure test])
  (:import [org.apache.storm.topology TopologyBuilder])
  (:import [org.apache.storm.generated InvalidTopologyException SubmitOptions TopologyInitialStatus])
  (:import [org.apache.storm.testing TestWordCounter TestWordSpout TestGlobalCount
            TestAggregatesCounter TestConfBolt AckFailMapTracker PythonShellMetricsBolt PythonShellMetricsSpout])
  (:import [org.apache.storm.task ShellBolt])
  (:import [org.apache.storm.spout ShellSpout])
  (:import [org.apache.storm.metric.api CountMetric IMetricsConsumer$DataPoint IMetricsConsumer$TaskInfo])
  (:import [org.apache.storm.metric.api.rpc CountShellMetric])
  (:import [org.apache.storm.utils Utils])
  
  (:use [org.apache.storm testing clojure config])
  (:use [org.apache.storm.daemon common])
  (:use [org.apache.storm.metric testing])
  (:require [org.apache.storm [thrift :as thrift]]))

(defbolt acking-bolt {} {:prepare true}
  [conf context collector]  
  (bolt
   (execute [tuple]
            (ack! collector tuple))))

(defbolt ack-every-other {} {:prepare true}
  [conf context collector]
  (let [state (atom -1)]
    (bolt
      (execute [tuple]
        (let [val (swap! state -)]
          (when (pos? val)
            (ack! collector tuple)
            ))))))

(defn assert-loop [afn ids]
  (while-timeout TEST-TIMEOUT-MS (not (every? afn ids))
    (Thread/sleep 1)))

(defn assert-acked [tracker & ids]
  (assert-loop #(.isAcked tracker %) ids))

(defn assert-failed [tracker & ids]
  (assert-loop #(.isFailed tracker %) ids))

(defbolt count-acks {} {:prepare true}
  [conf context collector]

  (let [mycustommetric (CountMetric.)]   
    (.registerMetric context "my-custom-metric" mycustommetric 5)
    (bolt
     (execute [tuple]
              (.incr mycustommetric)
              (ack! collector tuple)))))

(def metrics-data org.apache.storm.metric.testing/buffer)

(defn wait-for-atleast-N-buckets! [N comp-id metric-name cluster]
  (while-timeout TEST-TIMEOUT-MS
      (let [taskid->buckets (-> @metrics-data (get comp-id) (get metric-name))]
        (or
         (and (not= N 0) (nil? taskid->buckets))
         (not-every? #(<= N %) (map (comp count second) taskid->buckets))))
      ;;(log-message "Waiting for at least " N " timebuckets to appear in FakeMetricsConsumer for component id " comp-id " and metric name " metric-name " metrics " (-> @metrics-data (get comp-id) (get metric-name)))
    (if cluster
      (advance-cluster-time cluster 1)
      (Thread/sleep 10))))
    

(defn lookup-bucket-by-comp-id-&-metric-name! [comp-id metric-name]
  (-> @metrics-data
      (get comp-id)
      (get metric-name)
      (first) ;; pick first task in the list, ignore other tasks' metric data.
      (second)
      (or [])))

(defmacro assert-buckets! [comp-id metric-name expected cluster]
  `(do
     (let [N# (count ~expected)]
       (wait-for-atleast-N-buckets! N# ~comp-id ~metric-name ~cluster)
       (is (= ~expected (subvec (lookup-bucket-by-comp-id-&-metric-name! ~comp-id ~metric-name) 0 N#))))))

(defmacro assert-metric-data-exists! [comp-id metric-name]
  `(is (not-empty (lookup-bucket-by-comp-id-&-metric-name! ~comp-id ~metric-name))))

(deftest test-custom-metric
  (with-simulated-time-local-cluster
    [cluster :daemon-conf {TOPOLOGY-METRICS-CONSUMER-REGISTER
                           [{"class" "clojure.storm.metric.testing.FakeMetricConsumer"}]
                           "storm.zookeeper.connection.timeout" 30000
                           "storm.zookeeper.session.timeout" 60000
                           }]
    (let [feeder (feeder-spout ["field1"])
          topology (thrift/mk-topology
                    {"1" (thrift/mk-spout-spec feeder)}
                    {"2" (thrift/mk-bolt-spec {"1" :global} count-acks)})]      
      (submit-local-topology (:nimbus cluster) "metrics-tester" {} topology)

      (.feed feeder ["a"] 1)
      (advance-cluster-time cluster 6)
      (assert-buckets! "2" "my-custom-metric" [1] cluster)
            
      (advance-cluster-time cluster 5)
      (assert-buckets! "2" "my-custom-metric" [1 0] cluster)

      (advance-cluster-time cluster 20)
      (assert-buckets! "2" "my-custom-metric" [1 0 0 0 0 0] cluster)
      
      (.feed feeder ["b"] 2)
      (.feed feeder ["c"] 3)               
      (advance-cluster-time cluster 5)
      (assert-buckets! "2" "my-custom-metric" [1 0 0 0 0 0 2] cluster))))

(deftest test-custom-metric-with-multi-tasks
  (with-simulated-time-local-cluster
    [cluster :daemon-conf {TOPOLOGY-METRICS-CONSUMER-REGISTER
                           [{"class" "clojure.storm.metric.testing.FakeMetricConsumer"}]
                           "storm.zookeeper.connection.timeout" 30000
                           "storm.zookeeper.session.timeout" 60000
                           }]
    (let [feeder (feeder-spout ["field1"])
          topology (thrift/mk-topology
                     {"1" (thrift/mk-spout-spec feeder)}
                     {"2" (thrift/mk-bolt-spec {"1" :all} count-acks :p 1 :conf {TOPOLOGY-TASKS 2})})]
      (submit-local-topology (:nimbus cluster) "metrics-tester-with-multitasks" {} topology)

      (.feed feeder ["a"] 1)
      (advance-cluster-time cluster 6)
      (assert-buckets! "2" "my-custom-metric" [1] cluster)

      (advance-cluster-time cluster 5)
      (assert-buckets! "2" "my-custom-metric" [1 0] cluster)

      (advance-cluster-time cluster 20)
      (assert-buckets! "2" "my-custom-metric" [1 0 0 0 0 0] cluster)

      (.feed feeder ["b"] 2)
      (.feed feeder ["c"] 3)
      (advance-cluster-time cluster 5)
      (assert-buckets! "2" "my-custom-metric" [1 0 0 0 0 0 2] cluster))))

(defn mk-shell-bolt-with-metrics-spec
  [inputs command & kwargs]
  (let [command (into-array String command)]
    (apply thrift/mk-bolt-spec inputs
         (PythonShellMetricsBolt. command) kwargs)))

(deftest test-custom-metric-with-multilang-py
  (with-simulated-time-local-cluster 
    [cluster :daemon-conf {TOPOLOGY-METRICS-CONSUMER-REGISTER
                       [{"class" "clojure.storm.metric.testing.FakeMetricConsumer"}]
                       "storm.zookeeper.connection.timeout" 30000
                       "storm.zookeeper.session.timeout" 60000
                       }]
    (let [feeder (feeder-spout ["field1"])
          topology (thrift/mk-topology
                     {"1" (thrift/mk-spout-spec feeder)}
                     {"2" (mk-shell-bolt-with-metrics-spec {"1" :global} ["python" "tester_bolt_metrics.py"])})]
      (submit-local-topology (:nimbus cluster) "shell-metrics-tester" {} topology)

      (.feed feeder ["a"] 1)
      (advance-cluster-time cluster 6)
      (assert-buckets! "2" "my-custom-shell-metric" [1] cluster)
            
      (advance-cluster-time cluster 5)
      (assert-buckets! "2" "my-custom-shell-metric" [1 0] cluster)

      (advance-cluster-time cluster 20)
      (assert-buckets! "2" "my-custom-shell-metric" [1 0 0 0 0 0] cluster)
      
      (.feed feeder ["b"] 2)
      (.feed feeder ["c"] 3)               
      (advance-cluster-time cluster 5)
      (assert-buckets! "2" "my-custom-shell-metric" [1 0 0 0 0 0 2] cluster)
      )))

(defn mk-shell-spout-with-metrics-spec
  [command & kwargs]
  (let [command (into-array String command)]
    (apply thrift/mk-spout-spec (PythonShellMetricsSpout. command) kwargs)))

(deftest test-custom-metric-with-spout-multilang-py
  (with-simulated-time-local-cluster 
    [cluster :daemon-conf {TOPOLOGY-METRICS-CONSUMER-REGISTER
                       [{"class" "clojure.storm.metric.testing.FakeMetricConsumer"}]
                       "storm.zookeeper.connection.timeout" 30000
                       "storm.zookeeper.session.timeout" 60000}]
    (let [topology (thrift/mk-topology
                     {"1" (mk-shell-spout-with-metrics-spec ["python" "tester_spout_metrics.py"])}
                     {"2" (thrift/mk-bolt-spec {"1" :all} count-acks)})]
      (submit-local-topology (:nimbus cluster) "shell-spout-metrics-tester" {} topology)

      (advance-cluster-time cluster 7)
      (assert-buckets! "1" "my-custom-shellspout-metric" [2] cluster)
      )))


(deftest test-builtin-metrics-1
  (with-simulated-time-local-cluster
    [cluster :daemon-conf {TOPOLOGY-METRICS-CONSUMER-REGISTER                    
                           [{"class" "clojure.storm.metric.testing.FakeMetricConsumer"}]
                           TOPOLOGY-STATS-SAMPLE-RATE 1.0
                           TOPOLOGY-BUILTIN-METRICS-BUCKET-SIZE-SECS 60}]
    (let [feeder (feeder-spout ["field1"])
          topology (thrift/mk-topology
                    {"myspout" (thrift/mk-spout-spec feeder)}
                    {"mybolt" (thrift/mk-bolt-spec {"myspout" :shuffle} acking-bolt)})]      
      (submit-local-topology (:nimbus cluster) "metrics-tester" {} topology)
      
      (.feed feeder ["a"] 1)
      (advance-cluster-time cluster 61)
      (assert-buckets! "myspout" "__ack-count/default" [1] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [1] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [1] cluster)            
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [1] cluster)
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [1] cluster)

      (advance-cluster-time cluster 120)
      (assert-buckets! "myspout" "__ack-count/default" [1 0 0] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [1 0 0] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [1 0 0] cluster)
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [1 0 0] cluster)
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [1 0 0] cluster)

      (.feed feeder ["b"] 1)
      (.feed feeder ["c"] 1)
      (advance-cluster-time cluster 60)
      (assert-buckets! "myspout" "__ack-count/default" [1 0 0 2] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [1 0 0 2] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [1 0 0 2] cluster)      
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [1 0 0 2] cluster)
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [1 0 0 2] cluster))))


(deftest test-builtin-metrics-2
  (with-simulated-time-local-cluster
    [cluster :daemon-conf {TOPOLOGY-METRICS-CONSUMER-REGISTER
                           [{"class" "clojure.storm.metric.testing.FakeMetricConsumer"}]
                           TOPOLOGY-STATS-SAMPLE-RATE 1.0
                           TOPOLOGY-BUILTIN-METRICS-BUCKET-SIZE-SECS 5}]
    (let [feeder (feeder-spout ["field1"])
          tracker (AckFailMapTracker.)
          _ (.setAckFailDelegate feeder tracker)
          topology (thrift/mk-topology
                    {"myspout" (thrift/mk-spout-spec feeder)}
                    {"mybolt" (thrift/mk-bolt-spec {"myspout" :shuffle} ack-every-other)})]      
      (submit-local-topology (:nimbus cluster)
                             "metrics-tester"
                             {}
                             topology)
      
      (.feed feeder ["a"] 1)
      (advance-cluster-time cluster 6)
      (assert-acked tracker 1)
      (assert-buckets! "myspout" "__fail-count/default" [] cluster)
      (assert-buckets! "myspout" "__ack-count/default" [1] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [1] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [1] cluster)            
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [1] cluster)     
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [1] cluster)

      (.feed feeder ["b"] 2)      
      (advance-cluster-time cluster 5)
      (assert-buckets! "myspout" "__fail-count/default" [] cluster)
      (assert-buckets! "myspout" "__ack-count/default" [1 0] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [1 1] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [1 1] cluster)                  
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [1 0] cluster)
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [1 1] cluster)

      (advance-cluster-time cluster 15)      
      (assert-buckets! "myspout" "__ack-count/default" [1 0 0 0 0] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [1 1 0 0 0] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [1 1 0 0 0] cluster)
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [1 0 0 0 0] cluster)
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [1 1 0 0 0] cluster)
      
      (.feed feeder ["c"] 3)            
      (advance-cluster-time cluster 15)      
      (assert-buckets! "myspout" "__ack-count/default" [1 0 0 0 0 1 0 0] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [1 1 0 0 0 1 0 0] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [1 1 0 0 0 1 0 0] cluster)
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [1 0 0 0 0 1 0 0] cluster)
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [1 1 0 0 0 1 0 0] cluster))))

(deftest test-builtin-metrics-3
  (with-simulated-time-local-cluster
    [cluster :daemon-conf {TOPOLOGY-METRICS-CONSUMER-REGISTER
                           [{"class" "clojure.storm.metric.testing.FakeMetricConsumer"}]
                           TOPOLOGY-STATS-SAMPLE-RATE 1.0
                           TOPOLOGY-BUILTIN-METRICS-BUCKET-SIZE-SECS 5
                           TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS true}]
    (let [feeder (feeder-spout ["field1"])
          tracker (AckFailMapTracker.)
          _ (.setAckFailDelegate feeder tracker)
          topology (thrift/mk-topology
                    {"myspout" (thrift/mk-spout-spec feeder)}
                    {"mybolt" (thrift/mk-bolt-spec {"myspout" :global} ack-every-other)})]      
      (submit-local-topology (:nimbus cluster)
                             "timeout-tester"
                             {TOPOLOGY-MESSAGE-TIMEOUT-SECS 10}
                             topology)
      (.feed feeder ["a"] 1)
      (.feed feeder ["b"] 2)
      (.feed feeder ["c"] 3)
      (advance-cluster-time cluster 9)
      (assert-acked tracker 1 3)
      (assert-buckets! "myspout" "__ack-count/default" [2] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [3] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [3] cluster)
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [2] cluster)
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [3] cluster)
      
      (is (not (.isFailed tracker 2)))
      (advance-cluster-time cluster 30)
      (assert-failed tracker 2)
      (assert-buckets! "myspout" "__fail-count/default" [1] cluster)
      (assert-buckets! "myspout" "__ack-count/default" [2 0 0 0] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [3 0 0 0] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [3 0 0 0] cluster)
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [2 0 0 0] cluster)
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [3 0 0 0] cluster))))

(deftest test-system-bolt
  (with-simulated-time-local-cluster
    [cluster :daemon-conf {TOPOLOGY-METRICS-CONSUMER-REGISTER
                           [{"class" "clojure.storm.metric.testing.FakeMetricConsumer"}]
                           TOPOLOGY-BUILTIN-METRICS-BUCKET-SIZE-SECS 60}]
    (let [feeder (feeder-spout ["field1"])
          topology (thrift/mk-topology
                    {"1" (thrift/mk-spout-spec feeder)}
                    {})]      
      (submit-local-topology (:nimbus cluster) "metrics-tester" {} topology)

      (.feed feeder ["a"] 1)
      (advance-cluster-time cluster 70)
      (assert-buckets! "__system" "newWorkerEvent" [1] cluster)
      (assert-metric-data-exists! "__system" "uptimeSecs")
      (assert-metric-data-exists! "__system" "startTimeSecs")

      (advance-cluster-time cluster 180)
      (assert-buckets! "__system" "newWorkerEvent" [1 0 0 0] cluster)
      )))


