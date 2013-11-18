(ns backtype.storm.utils-test
  (:import [backtype.storm Config])
  (:import [backtype.storm.utils NimbusClient Utils])
  (:import [com.netflix.curator.retry ExponentialBackoffRetry])
  (:import [org.apache.thrift7.transport TTransportException])
  (:use [backtype.storm config util])
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

(deftest test-getConfiguredClient-throws-RunTimeException-on-bad-config
  (let [storm-conf (merge (read-storm-config)
                     {STORM-THRIFT-TRANSPORT-PLUGIN
                       "backtype.storm.security.auth.SimpleTransportPlugin"
                      Config/NIMBUS_HOST ""
                      Config/NIMBUS_THRIFT_PORT 65535
                     })]
    (is (thrown? RuntimeException
      (NimbusClient/getConfiguredClient storm-conf)))
  )
)

(deftest test-getConfiguredClient-throws-RunTimeException-on-bad-args
  (let [storm-conf (read-storm-config)]
    (is (thrown? TTransportException
      (NimbusClient. storm-conf "" 65535)
    ))
  )
)

(deftest test-add-to-classpath
  (is (= (add-to-classpath "a:b", ["", nil]) "a:b"))
  (is (= (add-to-classpath "a:b", ["c", nil]) "a:b:c"))
  (is (= (add-to-classpath "a:b", ["c", "d:e"]) "a:b:c:d:e"))
)


