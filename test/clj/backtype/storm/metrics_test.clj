(ns backtype.storm.metrics-test
  (:use [clojure test])
  (:import [backtype.storm.topology TopologyBuilder])
  (:import [backtype.storm.generated InvalidTopologyException SubmitOptions TopologyInitialStatus])
  (:import [backtype.storm.testing TestWordCounter TestWordSpout TestGlobalCount
            TestAggregatesCounter TestConfBolt AckFailMapTracker])
  (:import [backtype.storm.metric.api CountMetric IMetricsConsumer$DataPoint IMetricsConsumer$TaskInfo])
  
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm.metric testing]))


(bootstrap)

(defbolt count-acks {} {:prepare true}
  [conf context collector]

  (let [ack-count (CountMetric.)]   
    (.registerMetric context "ack-count" ack-count 5)
    (bolt
     (execute [tuple]
              (.inc ack-count)
              (ack! collector tuple)))))

(def datapoints-buffer (atom nil))

(defn metric-name->vals! [name]
  (->>  @datapoints-buffer
        (mapcat (fn [[task-info data-points]] data-points))
        (filter #(= name (.name %)))
        (map #(.value %))
        (into [])))

(deftest test-time-buckets
  (with-simulated-time-local-cluster
    [cluster :daemon-conf {TOPOLOGY-METRICS-CONSUMER-REGISTER
                           [{"class" "clojure.storm.metric.testing.FakeMetricConsumer"
                             "argument" {:ns (.ns #'datapoints-buffer) :var-name 'datapoints-buffer}}]}]
    (let [feeder (feeder-spout ["field1"])
          tracker (AckFailMapTracker.)
          _ (.setAckFailDelegate feeder tracker)
          topology (thrift/mk-topology
                    {"1" (thrift/mk-spout-spec feeder)}
                    {"2" (thrift/mk-bolt-spec {"1" :global} count-acks)})]      
      (submit-local-topology (:nimbus cluster) "metrics-tester" {} topology)
      
      (.feed feeder ["a"] 1)
      (advance-cluster-time cluster 6)      
      (is (= [1] (metric-name->vals! "ack-count")))
      
      (advance-cluster-time cluster 5)
      (is (= [1 0] (metric-name->vals! "ack-count")))

      (advance-cluster-time cluster 20)
      (is (= [1 0 0 0 0 0] (metric-name->vals! "ack-count")))
                  
      (.feed feeder ["b"] 2)
      (.feed feeder ["c"] 3)               
      (advance-cluster-time cluster 5)
      (is (= [1 0 0 0 0 0 2] (metric-name->vals! "ack-count"))))))

