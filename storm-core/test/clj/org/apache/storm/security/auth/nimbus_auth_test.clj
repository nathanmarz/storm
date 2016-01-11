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
(ns org.apache.storm.security.auth.nimbus-auth-test
  (:use [clojure test])
  (:require [org.apache.storm [testing :as testing]])
  (:require [org.apache.storm.daemon [nimbus :as nimbus]])
  (:require [org.apache.storm [zookeeper :as zk]])
  (:require [org.apache.storm.security.auth [auth-test :refer [nimbus-timeout]]])
  (:import [java.nio ByteBuffer])
  (:import [org.apache.storm Config])
  (:import [org.apache.storm.utils NimbusClient])
  (:import [org.apache.storm.generated NotAliveException])
  (:import [org.apache.storm.security.auth AuthUtils ThriftServer ThriftClient 
                                         ReqContext ThriftConnectionType])
  (:use [org.apache.storm cluster util config log])
  (:use [org.apache.storm.daemon common nimbus])
  (:import [org.apache.storm.generated Nimbus Nimbus$Client Nimbus$Processor 
            AuthorizationException SubmitOptions TopologyInitialStatus KillOptions])
  (:require [conjure.core])
  (:use [conjure core]))

(defn launch-test-cluster [nimbus-port login-cfg aznClass transportPluginClass] 
  (let [conf {NIMBUS-AUTHORIZER aznClass 
              NIMBUS-THRIFT-PORT nimbus-port
              STORM-THRIFT-TRANSPORT-PLUGIN transportPluginClass }
        conf (if login-cfg (merge conf {"java.security.auth.login.config" login-cfg}) conf)
        cluster-map (testing/mk-local-storm-cluster :supervisors 0
                                            :ports-per-supervisor 0
                                            :daemon-conf conf)
        nimbus-server (ThriftServer. (:daemon-conf cluster-map)
                                     (Nimbus$Processor. (:nimbus cluster-map)) 
                                     ThriftConnectionType/NIMBUS)]
    (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.stop nimbus-server))))
    (.start (Thread. #(.serve nimbus-server)))
    (testing/wait-for-condition #(.isServing nimbus-server))
    [cluster-map nimbus-server]))

(defmacro with-test-cluster [args & body]
  `(let [[cluster-map#  nimbus-server#] (launch-test-cluster  ~@args)]
      ~@body
      (log-debug "Shutdown cluster from macro")
      (testing/kill-local-storm-cluster cluster-map#)
      (.stop nimbus-server#)))

(deftest Simple-authentication-test
  (let [port (available-port)]
    (with-test-cluster [port nil nil "org.apache.storm.security.auth.SimpleTransportPlugin"]
      (let [storm-conf (merge (read-storm-config)
                              {STORM-THRIFT-TRANSPORT-PLUGIN "org.apache.storm.security.auth.SimpleTransportPlugin"
                               STORM-NIMBUS-RETRY-TIMES 0})
            client (NimbusClient. storm-conf "localhost" port nimbus-timeout)
            nimbus_client (.getClient client)]
        (testing "(Positive authorization) Simple protocol w/o authentication/authorization enforcement"
                 (is (thrown-cause? NotAliveException
                              (.activate nimbus_client "topo-name"))))
        (.close client)))))

(deftest test-noop-authorization-w-simple-transport
  (let [port (available-port)]
    (with-test-cluster [port nil
                  "org.apache.storm.security.auth.authorizer.NoopAuthorizer"
                  "org.apache.storm.security.auth.SimpleTransportPlugin"]
      (let [storm-conf (merge (read-storm-config)
                               {STORM-THRIFT-TRANSPORT-PLUGIN "org.apache.storm.security.auth.SimpleTransportPlugin"
                                STORM-NIMBUS-RETRY-TIMES 0})
            client (NimbusClient. storm-conf "localhost" port nimbus-timeout)
            nimbus_client (.getClient client)]
        (testing "(Positive authorization) Authorization plugin should accept client request"
                 (is (thrown-cause? NotAliveException
                              (.activate nimbus_client "topo-name"))))
        (.close client)))))

(deftest test-deny-authorization-w-simple-transport
  (let [port (available-port)]
    (with-test-cluster [port nil
                  "org.apache.storm.security.auth.authorizer.DenyAuthorizer"
                  "org.apache.storm.security.auth.SimpleTransportPlugin"]
      (let [storm-conf (merge (read-storm-config)
                               {STORM-THRIFT-TRANSPORT-PLUGIN "org.apache.storm.security.auth.SimpleTransportPlugin"
                               Config/NIMBUS_THRIFT_PORT port
                               STORM-NIMBUS-RETRY-TIMES 0})
            client (NimbusClient. storm-conf "localhost" port nimbus-timeout)
            nimbus_client (.getClient client)
            topologyInitialStatus (TopologyInitialStatus/findByValue 2)
            submitOptions (SubmitOptions. topologyInitialStatus)]
        (is (thrown-cause? AuthorizationException (.submitTopology nimbus_client  "topo-name" nil nil nil)))
        (is (thrown-cause? AuthorizationException (.submitTopologyWithOpts nimbus_client  "topo-name" nil nil nil submitOptions)))
        (is (thrown-cause? AuthorizationException (.beginFileUpload nimbus_client)))

        (is (thrown-cause? AuthorizationException (.uploadChunk nimbus_client nil nil)))
        (is (thrown-cause? AuthorizationException (.finishFileUpload nimbus_client nil)))
        (is (thrown-cause? AuthorizationException (.beginFileDownload nimbus_client nil)))
        (is (thrown-cause? AuthorizationException (.downloadChunk nimbus_client nil)))
        (is (thrown-cause? AuthorizationException (.getNimbusConf nimbus_client)))
        (is (thrown-cause? AuthorizationException (.getClusterInfo nimbus_client)))
        (stubbing [nimbus/check-storm-active! nil
                   nimbus/try-read-storm-conf-from-name {}]
          (is (thrown-cause? AuthorizationException (.killTopology nimbus_client "topo-name")))
          (is (thrown-cause? AuthorizationException (.killTopologyWithOpts nimbus_client "topo-name" (KillOptions.))))
          (is (thrown-cause? AuthorizationException (.activate nimbus_client "topo-name")))
          (is (thrown-cause? AuthorizationException (.deactivate nimbus_client "topo-name")))
          (is (thrown-cause? AuthorizationException (.rebalance nimbus_client "topo-name" nil)))
        )
        (stubbing [nimbus/try-read-storm-conf {}]
          (is (thrown-cause? AuthorizationException (.getTopologyConf nimbus_client "topo-ID")))
          (is (thrown-cause? AuthorizationException (.getTopology nimbus_client "topo-ID")))
          (is (thrown-cause? AuthorizationException (.getUserTopology nimbus_client "topo-ID")))
          (is (thrown-cause? AuthorizationException (.getTopologyInfo nimbus_client "topo-ID"))))
        (.close client)))))

(deftest test-noop-authorization-w-sasl-digest
  (let [port (available-port)]
    (with-test-cluster [port
                  "test/clj/org/apache/storm/security/auth/jaas_digest.conf"
                  "org.apache.storm.security.auth.authorizer.NoopAuthorizer"
                  "org.apache.storm.security.auth.digest.DigestSaslTransportPlugin"]
      (let [storm-conf (merge (read-storm-config)
                              {STORM-THRIFT-TRANSPORT-PLUGIN "org.apache.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/org/apache/storm/security/auth/jaas_digest.conf"
                               Config/NIMBUS_THRIFT_PORT port
                               STORM-NIMBUS-RETRY-TIMES 0})
            client (NimbusClient. storm-conf "localhost" port nimbus-timeout)
            nimbus_client (.getClient client)]
        (testing "(Positive authorization) Authorization plugin should accept client request"
                 (is (thrown-cause? NotAliveException
                              (.activate nimbus_client "topo-name"))))
        (.close client)))))

(deftest test-deny-authorization-w-sasl-digest
  (let [port (available-port)]
    (with-test-cluster [port
                  "test/clj/org/apache/storm/security/auth/jaas_digest.conf"
                  "org.apache.storm.security.auth.authorizer.DenyAuthorizer"
                  "org.apache.storm.security.auth.digest.DigestSaslTransportPlugin"]
      (let [storm-conf (merge (read-storm-config)
                              {STORM-THRIFT-TRANSPORT-PLUGIN "org.apache.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/org/apache/storm/security/auth/jaas_digest.conf"
                               Config/NIMBUS_THRIFT_PORT port
                               STORM-NIMBUS-RETRY-TIMES 0})
            client (NimbusClient. storm-conf "localhost" port nimbus-timeout)
            nimbus_client (.getClient client)
            topologyInitialStatus (TopologyInitialStatus/findByValue 2)
            submitOptions (SubmitOptions. topologyInitialStatus)]
        (is (thrown-cause? AuthorizationException (.submitTopology nimbus_client  "topo-name" nil nil nil)))
        (is (thrown-cause? AuthorizationException (.submitTopologyWithOpts nimbus_client  "topo-name" nil nil nil submitOptions)))
        (is (thrown-cause? AuthorizationException (.beginFileUpload nimbus_client)))
        (is (thrown-cause? AuthorizationException (.uploadChunk nimbus_client nil nil)))
        (is (thrown-cause? AuthorizationException (.finishFileUpload nimbus_client nil)))
        (is (thrown-cause? AuthorizationException (.beginFileDownload nimbus_client nil)))
        (is (thrown-cause? AuthorizationException (.downloadChunk nimbus_client nil)))
        (is (thrown-cause? AuthorizationException (.getNimbusConf nimbus_client)))
        (is (thrown-cause? AuthorizationException (.getClusterInfo nimbus_client)))
        (stubbing [nimbus/check-storm-active! nil
                   nimbus/try-read-storm-conf-from-name {}]
          (is (thrown-cause? AuthorizationException (.killTopology nimbus_client "topo-name")))
          (is (thrown-cause? AuthorizationException (.killTopologyWithOpts nimbus_client "topo-name" (KillOptions.))))
          (is (thrown-cause? AuthorizationException (.activate nimbus_client "topo-name")))
          (is (thrown-cause? AuthorizationException (.deactivate nimbus_client "topo-name")))
          (is (thrown-cause? AuthorizationException (.rebalance nimbus_client "topo-name" nil))))
        (stubbing [nimbus/try-read-storm-conf {}]
          (is (thrown-cause? AuthorizationException (.getTopologyConf nimbus_client "topo-ID")))
          (is (thrown-cause? AuthorizationException (.getTopology nimbus_client "topo-ID")))
          (is (thrown-cause? AuthorizationException (.getUserTopology nimbus_client "topo-ID")))
          (is (thrown-cause? AuthorizationException (.getTopologyInfo nimbus_client "topo-ID"))))
        (.close client)))))

