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
(ns org.apache.storm.security.auth.SaslTransportPlugin-test
  (:use [clojure test])
  (import [org.apache.storm.security.auth SaslTransportPlugin$User])
)

(deftest test-User-name
  (let [nam "Andy"
        user (SaslTransportPlugin$User. nam)]
    (are [a b] (= a b)
      nam (.toString user)
      (.getName user) (.toString user)
      (.hashCode nam) (.hashCode user)
    )
  )
)

(deftest test-User-equals
  (let [nam "Andy"
        user1 (SaslTransportPlugin$User. nam)
        user2 (SaslTransportPlugin$User. nam)
        user3 (SaslTransportPlugin$User. "Bobby")]
    (is (-> user1 (.equals user1)))
    (is (-> user1 (.equals user2)))
    (is (not (-> user1 (.equals nil))))
    (is (not (-> user1 (.equals "Potato"))))
    (is (not (-> user1 (.equals user3))))
  )
)
