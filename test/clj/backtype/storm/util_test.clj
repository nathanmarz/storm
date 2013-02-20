(ns backtype.storm.util-test
  (:import [backtype.storm Config])
  (:import [backtype.storm.utils Utils])
  (:import [com.netflix.curator.retry ExponentialBackoffRetry])
  (:use [backtype.storm util])
  (:use [clojure test])
)

(deftest test-new-curator-uses-exponential-backoff
  (let [expected_interval 2400
        expected_retries 10
        expected_ceiling 5000
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
    ; It would be very unlikely for this to fail three times.
    (is (or
          (= (.getSleepTimeMs retry 10 0) expected_ceiling)
          (= (.getSleepTimeMs retry 10 0) expected_ceiling)
          (= (.getSleepTimeMs retry 10 0) expected_ceiling)
        ))
  )
)
