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
(ns org.apache.storm.submitter-test
  (:use [clojure test])
  (:use [org.apache.storm config testing])
  (:import [org.apache.storm StormSubmitter])
  )

(deftest test-md5-digest-secret-generation
  (testing "No payload or scheme are generated when already present"
    (let [conf {STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD "foobar:12345"
                STORM-ZOOKEEPER-AUTH-SCHEME "anything"}
          result (StormSubmitter/prepareZookeeperAuthentication conf)
          actual-payload (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)
          actual-scheme (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME)]
      (is (nil? actual-payload))
      (is (= "digest" actual-scheme))))

  (testing "Scheme is set to digest if not already."
    (let [conf {STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD "foobar:12345"}
          result (StormSubmitter/prepareZookeeperAuthentication conf)
          actual-payload (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)
          actual-scheme (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME)]
      (is (nil? actual-payload))
      (is (= "digest" actual-scheme))))

  (testing "A payload is generated when no payload is present."
    (let [conf {STORM-ZOOKEEPER-AUTH-SCHEME "anything"}
          result (StormSubmitter/prepareZookeeperAuthentication conf)
          actual-payload (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)
          actual-scheme (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME)]
      (is (not (clojure.string/blank? actual-payload)))
      (is (= "digest" actual-scheme))))

  (testing "A payload is generated when payload is not correctly formatted."
    (let [bogus-payload "not-a-valid-payload"
          conf {STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD bogus-payload
                STORM-ZOOKEEPER-AUTH-SCHEME "anything"}
          result (StormSubmitter/prepareZookeeperAuthentication conf)
          actual-payload (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)
          actual-scheme (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME)]
      (is (not (StormSubmitter/validateZKDigestPayload bogus-payload))) ; Is this test correct?
      (is (not (clojure.string/blank? actual-payload)))
      (is (= "digest" actual-scheme))))

  (testing "A payload is generated when payload is null."
    (let [conf {STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD nil
                STORM-ZOOKEEPER-AUTH-SCHEME "anything"}
          result (StormSubmitter/prepareZookeeperAuthentication conf)
          actual-payload (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)
          actual-scheme (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME)]
      (is (not (clojure.string/blank? actual-payload)))
      (is (= "digest" actual-scheme))))

  (testing "A payload is generated when payload is blank."
    (let [conf {STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD ""
                STORM-ZOOKEEPER-AUTH-SCHEME "anything"}
          result (StormSubmitter/prepareZookeeperAuthentication conf)
          actual-payload (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)
          actual-scheme (.get result STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME)]
      (is (not (clojure.string/blank? actual-payload)))
      (is (= "digest" actual-scheme)))))
