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
(ns backtype.storm.security.auth.AuthUtils-test
  (:import [backtype.storm.security.auth AuthUtils])
  (:import [java.io IOException])
  (:import [javax.security.auth.login AppConfigurationEntry Configuration])
  (:import [org.mockito Mockito])
  (:use [clojure test])
)

(deftest test-throws-on-missing-section
  (is (thrown? IOException
    (AuthUtils/get (Mockito/mock Configuration) "bogus-section" "")))
)

(defn- mk-mock-app-config-entry []
  (let [toRet (Mockito/mock AppConfigurationEntry)]
    (. (Mockito/when (.getOptions toRet)) thenReturn (hash-map))
    toRet
  )
)

(deftest test-returns-null-if-no-such-section
  (let [entry (mk-mock-app-config-entry)
        entries (into-array (.getClass entry) [entry])
        section "bogus-section"
        conf (Mockito/mock Configuration)]
    (. (Mockito/when (. conf getAppConfigurationEntry section ))
       thenReturn entries)
    (is (nil? (AuthUtils/get conf section "nonexistent-key")))
  )
)

(deftest test-returns-first-value-for-valid-key
  (let [k "the-key"
        expected "good-value"
        empty-entry (mk-mock-app-config-entry)
        bad-entry (Mockito/mock AppConfigurationEntry)
        good-entry (Mockito/mock AppConfigurationEntry)
        conf (Mockito/mock Configuration)]
    (. (Mockito/when (.getOptions bad-entry)) thenReturn {k "bad-value"})
    (. (Mockito/when (.getOptions good-entry)) thenReturn {k expected})
    (let [entries (into-array (.getClass empty-entry)
                    [empty-entry good-entry bad-entry])
          section "bogus-section"]
      (. (Mockito/when (. conf getAppConfigurationEntry section))
         thenReturn entries)
      (is (not (nil? (AuthUtils/get conf section k))))
      (is (= (AuthUtils/get conf section k) expected))
    )
  )
)
