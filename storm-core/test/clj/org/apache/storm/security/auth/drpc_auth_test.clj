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
(ns org.apache.storm.security.auth.drpc-auth-test
  (:use [clojure test])
  (:require [org.apache.storm.daemon [drpc :as drpc]])
  (:import [org.apache.storm.generated AuthorizationException
            DRPCExecutionException DistributedRPC$Processor
            DistributedRPCInvocations$Processor])
  (:import [org.apache.storm Config])
  (:import [org.apache.storm.security.auth ReqContext SingleUserPrincipal ThriftServer ThriftConnectionType])
  (:import [org.apache.storm.utils DRPCClient])
  (:import [org.apache.storm.drpc DRPCInvocationsClient])
  (:import [java.util.concurrent TimeUnit])
  (:import [javax.security.auth Subject])
  (:use [org.apache.storm util config log])
  (:use [org.apache.storm.daemon common])
  (:use [org.apache.storm testing]))

(def DRPC-TIMEOUT-SEC (* (/ TEST-TIMEOUT-MS 1000) 2))

(defn launch-server [conf drpcAznClass transportPluginClass login-cfg client-port invocations-port]
  (let [conf (if drpcAznClass (assoc conf DRPC-AUTHORIZER drpcAznClass) conf)
        conf (if transportPluginClass (assoc conf STORM-THRIFT-TRANSPORT-PLUGIN transportPluginClass) conf)
        conf (if login-cfg (assoc conf "java.security.auth.login.config" login-cfg) conf)
        conf (assoc conf DRPC-PORT client-port)
        conf (assoc conf DRPC-INVOCATIONS-PORT invocations-port)
        service-handler (drpc/service-handler conf)
        handler-server (ThriftServer. conf
                                      (DistributedRPC$Processor. service-handler)
                                      ThriftConnectionType/DRPC)
        invoke-server (ThriftServer. conf
                                     (DistributedRPCInvocations$Processor. service-handler)
                                     ThriftConnectionType/DRPC_INVOCATIONS)]
    (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.stop handler-server) (.stop invoke-server))))
    (log-message "storm conf:" conf)
    (log-message "Starting DRPC invocation server ...")
    (.start (Thread. #(.serve invoke-server)))
    (wait-for-condition #(.isServing invoke-server))
    (log-message "Starting DRPC handler server ...")
    (.start (Thread. #(.serve handler-server)))
    (wait-for-condition #(.isServing handler-server))
    [handler-server invoke-server]))

(defmacro with-server [args & body]
  `(let [[handler-server# invoke-server#] (launch-server ~@args)]
      ~@body
      (log-message "Stopping DRPC servers ...")
      (.stop handler-server#)
      (.stop invoke-server#)
      ))

(deftest deny-drpc-test
  (let [client-port (available-port)
        invocations-port (available-port (inc client-port))
        storm-conf (read-storm-config)]
    (with-server [storm-conf "org.apache.storm.security.auth.authorizer.DenyAuthorizer"
                  nil nil client-port invocations-port]
      (let [drpc (DRPCClient. storm-conf "localhost" client-port)
            drpc_client (.getClient drpc)
            invocations (DRPCInvocationsClient. storm-conf "localhost" invocations-port)
            invocations_client (.getClient invocations)]
        (is (thrown? AuthorizationException (.execute drpc_client "func-foo" "args-bar")))
        (is (thrown? AuthorizationException (.fetchRequest invocations_client nil)))
        (.close drpc)
        (.close invocations)))))

(deftest deny-drpc-digest-test
  (let [client-port (available-port)
        invocations-port (available-port (inc client-port))
        storm-conf (read-storm-config)]
    (with-server [storm-conf "org.apache.storm.security.auth.authorizer.DenyAuthorizer"
                  "org.apache.storm.security.auth.digest.DigestSaslTransportPlugin"
                  "test/clj/org/apache/storm/security/auth/jaas_digest.conf"
                  client-port invocations-port]
      (let [conf (merge storm-conf {STORM-THRIFT-TRANSPORT-PLUGIN "org.apache.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/org/apache/storm/security/auth/jaas_digest.conf"})
            drpc (DRPCClient. conf "localhost" client-port)
            drpc_client (.getClient drpc)
            invocations (DRPCInvocationsClient. conf "localhost" invocations-port)
            invocations_client (.getClient invocations)]
        (is (thrown? AuthorizationException (.execute drpc_client "func-foo" "args-bar")))
        (is (thrown? AuthorizationException (.fetchRequest invocations_client nil)))
        (.close drpc)
        (.close invocations)))))

(defmacro with-simple-drpc-test-scenario
  [[strict? alice-client bob-client charlie-client alice-invok charlie-invok] & body]
  (let [client-port (available-port)
        invocations-port (available-port (inc client-port))
        storm-conf (merge (read-storm-config)
                          {DRPC-AUTHORIZER-ACL-STRICT strict?
                           DRPC-AUTHORIZER-ACL-FILENAME "drpc-simple-acl-test-scenario.yaml"
                           STORM-THRIFT-TRANSPORT-PLUGIN "org.apache.storm.security.auth.digest.DigestSaslTransportPlugin"})]
    `(with-server [~storm-conf
                   "org.apache.storm.security.auth.authorizer.DRPCSimpleACLAuthorizer"
                   "org.apache.storm.security.auth.digest.DigestSaslTransportPlugin"
                   "test/clj/org/apache/storm/security/auth/drpc-auth-server.jaas"
                   ~client-port ~invocations-port]
       (let [~alice-client (DRPCClient.
                           (merge ~storm-conf {"java.security.auth.login.config"
                                              "test/clj/org/apache/storm/security/auth/drpc-auth-alice.jaas"})
                           "localhost"
                           ~client-port)
             ~bob-client (DRPCClient.
                         (merge ~storm-conf {"java.security.auth.login.config"
                                            "test/clj/org/apache/storm/security/auth/drpc-auth-bob.jaas"})
                         "localhost"
                         ~client-port)
             ~charlie-client (DRPCClient.
                               (merge ~storm-conf {"java.security.auth.login.config"
                                                  "test/clj/org/apache/storm/security/auth/drpc-auth-charlie.jaas"})
                               "localhost"
                               ~client-port)
             ~alice-invok (DRPCInvocationsClient.
                            (merge ~storm-conf {"java.security.auth.login.config"
                                               "test/clj/org/apache/storm/security/auth/drpc-auth-alice.jaas"})
                            "localhost"
                            ~invocations-port)
             ~charlie-invok (DRPCInvocationsClient.
                             (merge ~storm-conf {"java.security.auth.login.config"
                                                "test/clj/org/apache/storm/security/auth/drpc-auth-charlie.jaas"})
                             "localhost"
                             ~invocations-port)]
         (try
           ~@body
           (finally
             (.close ~alice-client)
             (.close ~bob-client)
             (.close ~charlie-client)
             (.close ~alice-invok)
             (.close ~charlie-invok)))))))

(deftest drpc-per-function-auth-strict-test
  (with-simple-drpc-test-scenario [true alice-client bob-client charlie-client alice-invok charlie-invok]
    (let [drpc-timeout-seconds DRPC-TIMEOUT-SEC]
      (testing "Permitted user can execute a function in the ACL"
        (let [func "jump"
              exec-ftr (future (.execute alice-client func "some args"))
              id (atom "")
              expected "Authorized DRPC"]
          (with-timeout drpc-timeout-seconds TimeUnit/SECONDS
            (while (empty? @id)
              (reset! id
                      (-> charlie-invok (.fetchRequest func) .get_request_id)))
          (.result charlie-invok @id expected)
          (is (= expected (.get exec-ftr drpc-timeout-seconds TimeUnit/SECONDS))))))

      (testing "execute fails when function is not in ACL"
        (is (thrown-cause? AuthorizationException
          (.execute alice-client "jog" "some args"))))

      (testing "fetchRequest fails when function is not in ACL"
        (is (thrown-cause? AuthorizationException
          (.fetchRequest charlie-invok "jog"))))

      (testing "authorized user can fail a request"
        (let [func "jump"
              exec-ftr (future (.execute alice-client func "some args"))
              id (atom "")]
          (with-timeout drpc-timeout-seconds TimeUnit/SECONDS
            (while (empty? @id)
              (reset! id
                      (-> charlie-invok (.fetchRequest func) .get_request_id)))
          (.failRequest charlie-invok @id)
          (is (thrown-cause? DRPCExecutionException
                             (.get exec-ftr drpc-timeout-seconds TimeUnit/SECONDS))))))

      (testing "unauthorized invocation user is denied returning a result"
        (let [func "jump"
              exec-ftr (future (.execute bob-client func "some args"))
              id (atom "")
              expected "Only Authorized User can populate the result"]
          (with-timeout drpc-timeout-seconds TimeUnit/SECONDS
            (while (empty? @id)
              (reset! id
                      (-> charlie-invok (.fetchRequest func) .get_request_id)))
          (is (thrown-cause? AuthorizationException
            (.result alice-invok @id "not the expected result")))
          (.result charlie-invok @id expected)
          (is (= expected (.get exec-ftr drpc-timeout-seconds TimeUnit/SECONDS))))))

      (testing "unauthorized invocation user is denied failing a request"
        (let [func "jump"
              exec-ftr (future (.execute alice-client func "some args"))
              id (atom "")]
          (with-timeout drpc-timeout-seconds TimeUnit/SECONDS
            (while (empty? @id)
              (reset! id
                      (-> charlie-invok (.fetchRequest func) .get_request_id)))
          (is (thrown-cause? AuthorizationException
            (.failRequest alice-invok @id)))
          (.failRequest charlie-invok @id))))

      (testing "unauthorized invocation user is denied fetching a request"
        (let [func "jump"
              exec-ftr (future (.execute bob-client func "some args"))
              id (atom "")
              expected "Only authorized users can fetchRequest"]
          (Thread/sleep 1000)
          (is (thrown-cause? AuthorizationException
            (-> alice-invok (.fetchRequest func) .get_request_id)))
          (with-timeout drpc-timeout-seconds TimeUnit/SECONDS
            (while (empty? @id)
              (reset! id
                      (-> charlie-invok (.fetchRequest func) .get_request_id)))
          (.result charlie-invok @id expected)
          (is (= expected (.get exec-ftr drpc-timeout-seconds TimeUnit/SECONDS)))))))))

(deftest drpc-per-function-auth-non-strict-test
  (with-simple-drpc-test-scenario [false alice-client bob-client charlie-client alice-invok charlie-invok]
    (let [drpc-timeout-seconds DRPC-TIMEOUT-SEC]
      (testing "Permitted user can execute a function in the ACL"
        (let [func "jump"
              exec-ftr (future (.execute alice-client func "some args"))
              id (atom "")
              expected "Authorized DRPC"]
          (with-timeout drpc-timeout-seconds TimeUnit/SECONDS
            (while (empty? @id)
              (reset! id
                      (-> charlie-invok (.fetchRequest func) .get_request_id)))
          (.result charlie-invok @id expected)
          (is (= expected (.get exec-ftr drpc-timeout-seconds TimeUnit/SECONDS))))))

      (testing "DRPC succeeds for anyone when function is not in ACL"
        (let [func "jog"
              exec-ftr (future (.execute charlie-client func "some args"))
              id (atom "")
              expected "Permissive/No ACL Entry"]
          (with-timeout drpc-timeout-seconds TimeUnit/SECONDS
            (while (empty? @id)
              (reset! id
                      (-> alice-invok (.fetchRequest func) .get_request_id)))
          (.result alice-invok @id expected)
          (is (= expected (.get exec-ftr drpc-timeout-seconds TimeUnit/SECONDS))))))

      (testing "failure of a request is allowed when function is not in ACL"
        (let [func "jog"
              exec-ftr (future (.execute charlie-client func "some args"))
              id (atom "")]
          (with-timeout drpc-timeout-seconds TimeUnit/SECONDS
            (while (empty? @id)
              (reset! id
                      (-> alice-invok (.fetchRequest func) .get_request_id)))
          (.failRequest alice-invok @id)
          (is (thrown-cause? DRPCExecutionException
                             (.get exec-ftr drpc-timeout-seconds TimeUnit/SECONDS))))))

      (testing "authorized user can fail a request"
        (let [func "jump"
              exec-ftr (future (.execute alice-client func "some args"))
              id (atom "")]
          (with-timeout drpc-timeout-seconds TimeUnit/SECONDS
            (while (empty? @id)
              (reset! id
                      (-> charlie-invok (.fetchRequest func) .get_request_id)))
          (.failRequest charlie-invok @id)
          (is (thrown-cause? DRPCExecutionException
                             (.get exec-ftr drpc-timeout-seconds TimeUnit/SECONDS))))))

      (testing "unauthorized invocation user is denied returning a result"
        (let [func "jump"
              exec-ftr (future (.execute bob-client func "some args"))
              id (atom "")
              expected "Only Authorized User can populate the result"]
          (with-timeout drpc-timeout-seconds TimeUnit/SECONDS
            (while (empty? @id)
              (reset! id
                      (-> charlie-invok (.fetchRequest func) .get_request_id)))
          (is (thrown-cause? AuthorizationException
            (.result alice-invok @id "not the expected result")))
          (.result charlie-invok @id expected)
          (is (= expected (.get exec-ftr drpc-timeout-seconds TimeUnit/SECONDS))))))

      (testing "unauthorized invocation user is denied failing a request"
        (let [func "jump"
              exec-ftr (future (.execute alice-client func "some args"))
              id (atom "")]
          (with-timeout drpc-timeout-seconds TimeUnit/SECONDS
            (while (empty? @id)
              (reset! id
                      (-> charlie-invok (.fetchRequest func) .get_request_id)))
          (is (thrown-cause? AuthorizationException
            (.failRequest alice-invok @id)))
          (.failRequest charlie-invok @id))))

      (testing "unauthorized invocation user is denied fetching a request"
        (let [func "jump"
              exec-ftr (future (.execute bob-client func "some args"))
              id (atom "")
              expected "Only authorized users can fetchRequest"]
          (Thread/sleep 1000)
          (is (thrown-cause? AuthorizationException
            (-> alice-invok (.fetchRequest func) .get_request_id)))
          (with-timeout drpc-timeout-seconds TimeUnit/SECONDS
            (while (empty? @id)
              (reset! id
                      (-> charlie-invok (.fetchRequest func) .get_request_id)))
          (.result charlie-invok @id expected)
          (is (= expected (.get exec-ftr drpc-timeout-seconds TimeUnit/SECONDS)))))))))
