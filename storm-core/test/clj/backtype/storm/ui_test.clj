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
(ns backtype.storm.ui-test
  (:use [clojure test])
  (:use [backtype.storm config])
  (:use [backtype.storm testing])
  (:require [backtype.storm.ui [core :as core]])
  )

(deftest test-authorized-ui-user
  (testing "allow cluster admin"
    (let [conf {UI-FILTER "something" NIMBUS-ADMINS ["alice"]}]
      (is (core/authorized-ui-user? "alice" conf {}))))

  (testing "ignore any cluster-set topology.users"
    (let [conf {UI-FILTER "something" TOPOLOGY-USERS ["alice"]}]
      (is (not (core/authorized-ui-user? "alice" conf {})))))

  (testing "allow cluster ui user"
    (let [conf {UI-FILTER "something" UI-USERS ["alice"]}]
      (is (core/authorized-ui-user? "alice" conf {}))))

  (testing "allow submitted topology user"
    (let [topo-conf {TOPOLOGY-USERS ["alice"]}]
      (is (core/authorized-ui-user? "alice" {UI-FILTER "something"} topo-conf))))

  (testing "allow submitted ui user"
    (let [topo-conf {UI-USERS ["alice"]}]
      (is (core/authorized-ui-user? "alice" {UI-FILTER "something"} topo-conf))))

  (testing "disallow user not in nimbus admin, topo user, or ui user"
    (is (not (core/authorized-ui-user? "alice" {UI-FILTER "something"} {}))))

  (testing "user cannot override nimbus admin"
    (let [topo-conf {NIMBUS-ADMINS ["alice"]}]
      (is (not (core/authorized-ui-user? "alice" {UI-FILTER "something"} topo-conf))))))
