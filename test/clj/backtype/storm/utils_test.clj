(ns backtype.storm.utils-test
  (:import [backtype.storm Config])
  (:import [backtype.storm.utils Utils])
  (:import [com.netflix.curator.retry ExponentialBackoffRetry])
  (:use [backtype.storm util])
  (:use [clojure test])
)

(deftest test-new-curator-uses-exponential-backoff
  (let [expected_interval 2400
        expected_retries 10
        expected_ceiling (/ expected_interval 2)
        conf (merge (clojurify-structure (Utils/readDefaultConfig))
          {Config/STORM_ZOOKEEPER_RETRY_INTERVAL expected_interval
           Config/STORM_ZOOKEEPER_RETRY_TIMES expected_retries
           Config/STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING expected_ceiling})
        servers ["bogus_server"]
        arbitrary_port 42
        curator (Utils/newCurator conf servers arbitrary_port)
        retry (-> curator .getZookeeperClient .getRetryPolicy)
       ]
    (is (.isAssignableFrom ExponentialBackoffRetry (.getClass retry)))
    (is (= (.getBaseSleepTimeMs retry) expected_interval)) 
    (is (= (.getN retry) expected_retries)) 
    (is (= (.getMaxRetryInterval retry) expected_ceiling)) 
    (is (= (.getSleepTimeMs retry 10 0) expected_ceiling))
  )
)
