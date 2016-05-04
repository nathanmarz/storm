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
(ns org.apache.storm.security.auth.ReqContext-test
  (:import [org.apache.storm.security.auth ReqContext])
  (:import [java.net InetAddress])
  (:import [java.security AccessControlContext Principal])
  (:import [javax.security.auth Subject])
  (:use [clojure test])
)

(def test-subject
  (let [rc (ReqContext/context)
        expected (Subject.)]
    (is (not (.isReadOnly expected)))
    (.setSubject rc expected)
    (is (= (.subject rc) expected))

    ; Change the Subject by setting read-only.
    (.setReadOnly expected)
    (.setSubject rc expected)
    (is (= (.subject rc) expected))
  )
)

(deftest test-remote-address
  (let [rc (ReqContext/context)
        expected (InetAddress/getByAddress (.getBytes "ABCD"))]
    (.setRemoteAddress rc expected)
    (is (= (.remoteAddress rc) expected))
  )
)

(deftest test-principal-returns-null-when-no-subject
  (let [rc (ReqContext/context)]
    (.setSubject rc (Subject.))
    (is (nil? (.principal rc)))
  )
)

(def principal-name "Test Principal")

(defn TestPrincipal []
  (reify Principal
    (^String getName [this]
      principal-name)
  )
)

(deftest test-principal
  (let [p (TestPrincipal)
        principals (hash-set p)
        creds (hash-set)
        s (Subject. false principals creds, creds)
        rc (ReqContext/context)]
    (.setSubject rc s)
    (is (not (nil? (.principal rc))))
    (is (= (-> rc .principal .getName) principal-name))
    (.setSubject rc nil)
  )
)
