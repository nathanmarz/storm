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
(ns backtype.storm.utils-test
  (:import [backtype.storm Config])
  (:import [backtype.storm.utils NimbusClient Utils])
  (:import [org.apache.curator.retry ExponentialBackoffRetry])
  (:import [org.apache.thrift.transport TTransportException])
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

(deftest test-secs-to-millis-long
  (is (= 0 (secs-to-millis-long 0)))
  (is (= 2 (secs-to-millis-long 0.002)))
  (is (= 500 (secs-to-millis-long 0.5)))
  (is (= 1000 (secs-to-millis-long 1)))
  (is (= 1080 (secs-to-millis-long 1.08)))
  (is (= 10000 (secs-to-millis-long 10)))
  (is (= 10100 (secs-to-millis-long 10.1)))
)

