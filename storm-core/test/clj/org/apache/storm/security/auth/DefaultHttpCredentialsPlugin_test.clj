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
(ns org.apache.storm.security.auth.DefaultHttpCredentialsPlugin-test
  (:use [clojure test])
  (:import [javax.security.auth Subject])
  (:import [javax.servlet.http HttpServletRequest])
  (:import [org.apache.storm.security.auth SingleUserPrincipal])
  (:import [org.mockito Mockito])
  (:import [org.apache.storm.security.auth DefaultHttpCredentialsPlugin
            ReqContext SingleUserPrincipal])
  )

(deftest test-getUserName
  (let [handler (doto (DefaultHttpCredentialsPlugin.) (.prepare {}))]
    (testing "returns null when request is null"
      (is (nil? (.getUserName handler nil))))

    (testing "returns null when user principal is null"
      (let [req (Mockito/mock HttpServletRequest)]
        (is (nil? (.getUserName handler req)))))

    (testing "returns null when user is blank"
      (let [princ (SingleUserPrincipal. "")
            req (Mockito/mock HttpServletRequest)]
        (. (Mockito/when (. req getUserPrincipal))
           thenReturn princ)
        (is (nil? (.getUserName handler req)))))

    (testing "returns correct user from requests principal"
      (let [exp-name "Alice"
            princ (SingleUserPrincipal. exp-name)
            req (Mockito/mock HttpServletRequest)]
        (. (Mockito/when (. req getUserPrincipal))
           thenReturn princ)
        (is (.equals exp-name (.getUserName handler req)))))

    (testing "returns doAsUser from requests principal when Header has doAsUser param set"
      (try
        (let [exp-name "Alice"
              do-as-user-name "Bob"
              princ (SingleUserPrincipal. exp-name)
              req (Mockito/mock HttpServletRequest)
              _ (. (Mockito/when (. req getUserPrincipal))
                thenReturn princ)
              _ (. (Mockito/when (. req getHeader "doAsUser"))
                thenReturn do-as-user-name)
              context (.populateContext handler (ReqContext/context) req)]
          (is (= true (.isImpersonating context)))
          (is (.equals exp-name (.getName (.realPrincipal context))))
          (is (.equals do-as-user-name (.getName (.principal context)))))
        (finally
          (ReqContext/reset))))))

(deftest test-populate-req-context-on-null-user
  (try
    (let [req (Mockito/mock HttpServletRequest)
          handler (doto (DefaultHttpCredentialsPlugin.) (.prepare {}))
          subj (Subject. false (set [(SingleUserPrincipal. "test")]) (set []) (set []))
          context (ReqContext. subj)]
      (is (= 0 (-> handler (.populateContext context req) (.subject) (.getPrincipals) (.size)))))
    (finally
      (ReqContext/reset))))
