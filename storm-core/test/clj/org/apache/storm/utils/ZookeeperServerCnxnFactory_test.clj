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
(ns org.apache.storm.utils.ZookeeperServerCnxnFactory-test
  (:import [org.apache.storm.utils ZookeeperServerCnxnFactory])
  (:use [clojure test])
)

(deftest test-constructor-throws-runtimeexception-if-port-too-large
  (is (thrown? RuntimeException (ZookeeperServerCnxnFactory. 65536 42)))
)

(deftest test-factory
  (let [zkcf-negative (ZookeeperServerCnxnFactory. -42 42)
        next-port (+ (.port zkcf-negative) 1)
        arbitrary-max-clients 42
        zkcf-next (ZookeeperServerCnxnFactory. next-port arbitrary-max-clients)]
    ; Test handling negative port
    (is (not (nil? zkcf-negative)))
    ; Test max-clients is correctly set.
    (is (= (-> zkcf-next .factory .getMaxClientCnxnsPerHost) arbitrary-max-clients))
  )
)
