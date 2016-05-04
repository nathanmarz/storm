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
(ns org.apache.storm.security.auth.auto-login-module-test
  (:use [clojure test])
  (:use [org.apache.storm util])
  (:import [org.apache.storm.security.auth.kerberos AutoTGT
            AutoTGTKrb5LoginModule AutoTGTKrb5LoginModuleTest])
  (:import [javax.security.auth Subject Subject])
  (:import [javax.security.auth.kerberos KerberosTicket KerberosPrincipal])
  (:import [org.mockito Mockito])
  (:import [java.text SimpleDateFormat])
  (:import [java.util Date])
  (:import [java.util Arrays])
  (:import [java.net InetAddress])
  )

(deftest login-module-no-subj-no-tgt-test
  (testing "Behavior is correct when there is no Subject or TGT"
    (let [login-module (AutoTGTKrb5LoginModule.)]

      (is (thrown-cause? javax.security.auth.login.LoginException
                         (.login login-module)))
      (is (not (.commit login-module)))
      (is (not (.abort login-module)))
      (is (.logout login-module)))))

(deftest login-module-readonly-subj-no-tgt-test
  (testing "Behavior is correct when there is a read-only Subject and no TGT"
    (let [readonly-subj (Subject. true #{} #{} #{})
          login-module (AutoTGTKrb5LoginModule.)]
      (.initialize login-module readonly-subj nil nil nil)
      (is (not (.commit login-module)))
      (is (.logout login-module)))))

(deftest login-module-with-subj-no-tgt-test
  (testing "Behavior is correct when there is a Subject and no TGT"
    (let [login-module (AutoTGTKrb5LoginModule.)]
      (.initialize login-module (Subject.) nil nil nil)
      (is (thrown-cause? javax.security.auth.login.LoginException
                         (.login login-module)))
      (is (not (.commit login-module)))
      (is (not (.abort login-module)))
      (is (.logout login-module)))))

(deftest login-module-no-subj-with-tgt-test
  (testing "Behavior is correct when there is no Subject and a TGT"
    (let [login-module (AutoTGTKrb5LoginModuleTest.)]
      (.setKerbTicket login-module (Mockito/mock KerberosTicket))
      (is (.login login-module))
      (is (thrown-cause? javax.security.auth.login.LoginException
                         (.commit login-module)))

      (.setKerbTicket login-module (Mockito/mock KerberosTicket))
      (is (.abort login-module))
      (is (.logout login-module)))))

(deftest login-module-readonly-subj-with-tgt-test
  (testing "Behavior is correct when there is a read-only Subject and a TGT"
    (let [readonly-subj (Subject. true #{} #{} #{})
          login-module (AutoTGTKrb5LoginModuleTest.)]
      (.initialize login-module readonly-subj nil nil nil)
      (.setKerbTicket login-module (Mockito/mock KerberosTicket))
      (is (.login login-module))
      (is (thrown-cause? javax.security.auth.login.LoginException
                         (.commit login-module)))

      (.setKerbTicket login-module (Mockito/mock KerberosTicket))
      (is (.abort login-module))
      (is (.logout login-module)))))

(deftest login-module-with-subj-and-tgt
  (testing "Behavior is correct when there is a Subject and a TGT"
    (let [login-module (AutoTGTKrb5LoginModuleTest.)
          _ (set! (. login-module client) (Mockito/mock
                                            java.security.Principal))
          endTime (.parse (java.text.SimpleDateFormat. "ddMMyyyy") "31122030")
          asn1Enc (byte-array 10)
          _ (Arrays/fill asn1Enc (byte 122))
          sessionKey (byte-array 10)
          _ (Arrays/fill sessionKey (byte 123))
          ticket (KerberosTicket.
                   asn1Enc
                   (KerberosPrincipal. "client/localhost@local.com")
                   (KerberosPrincipal. "server/localhost@local.com")
                   sessionKey
                   234
                   (boolean-array (map even? (range 3 10)))
                   (Date.)
                   (Date.)
                   endTime,
                   endTime,
                   (into-array InetAddress [(InetAddress/getByName "localhost")]))]
      (.initialize login-module (Subject.) nil nil nil)
      (.setKerbTicket login-module ticket)
      (is (.login login-module))
      (is (.commit login-module))
      (is (.abort login-module))
      (is (.logout login-module)))))
