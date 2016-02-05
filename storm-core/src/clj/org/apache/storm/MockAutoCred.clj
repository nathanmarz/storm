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

;;mock implementation of INimbusCredentialPlugin,IAutoCredentials and ICredentialsRenewer for testing only.
(ns org.apache.storm.MockAutoCred
  (:use [org.apache.storm testing config])
  (:import [org.apache.storm.security.INimbusCredentialPlugin]
           [org.apache.storm.security.auth   ICredentialsRenewer])
  (:gen-class
    :implements [org.apache.storm.security.INimbusCredentialPlugin
                 org.apache.storm.security.auth.IAutoCredentials
                 org.apache.storm.security.auth.ICredentialsRenewer]))

(def nimbus-cred-key "nimbusCredTestKey")
(def nimbus-cred-val "nimbusTestCred")
(def nimbus-cred-renew-val "renewedNimbusTestCred")
(def gateway-cred-key "gatewayCredTestKey")
(def gateway-cred-val "gatewayTestCred")
(def gateway-cred-renew-val "renewedGatewayTestCred")

(defn -populateCredentials
  ([this creds conf]
  (.put creds nimbus-cred-key nimbus-cred-val))
  ([this creds]
  (.put creds gateway-cred-key gateway-cred-val)))

(defn -prepare
  [this conf])

(defn -renew
  [this cred conf]
  (.put cred nimbus-cred-key nimbus-cred-renew-val)
  (.put cred gateway-cred-key gateway-cred-renew-val))

(defn -populateSubject
  [subject credentials]
  (.add (.getPublicCredentials subject) (.get credentials nimbus-cred-key))
  (.add (.getPublicCredentials subject) (.get credentials gateway-cred-key)))

(defn -updateSubject
  [subject credentials]
  (-populateSubject subject credentials))



