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
(ns backtype.storm.security.auth.nimbus-auth-test
  (:use [clojure test])
  (:require [backtype.storm [testing :as testing]])
  (:require [backtype.storm.daemon [nimbus :as nimbus]])
  (:require [backtype.storm [zookeeper :as zk]])
  (:import [java.nio ByteBuffer])
  (:import [backtype.storm Config])
  (:import [backtype.storm.utils NimbusClient])
  (:import [backtype.storm.generated NotAliveException])
  (:import [backtype.storm.security.auth AuthUtils ThriftServer ThriftClient 
                                         ReqContext ThriftConnectionType])
  (:use [backtype.storm bootstrap cluster util])
  (:use [backtype.storm.daemon common nimbus])
  (:use [backtype.storm bootstrap])
  (:import [backtype.storm.generated Nimbus Nimbus$Client 
            AuthorizationException SubmitOptions TopologyInitialStatus KillOptions])
  (:require [conjure.core])
  (:use [conjure core])
  )

(bootstrap)

(def nimbus-timeout (Integer. 30))

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
    (wait-for-condition #(.isServing nimbus-server))
    [cluster-map nimbus-server]))

(defmacro with-test-cluster [args & body]
  `(let [[cluster-map#  nimbus-server#] (launch-test-cluster  ~@args)]
      ~@body
      (log-debug "Shutdown cluster from macro")
      (testing/kill-local-storm-cluster cluster-map#)
      (.stop nimbus-server#)))

(deftest Simple-authentication-test 
  (with-test-cluster [6627 nil nil "backtype.storm.security.auth.SimpleTransportPlugin"]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                             STORM-NIMBUS-RETRY-TIMES 0})
          client (NimbusClient. storm-conf "localhost" 6627 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Positive authorization) Simple protocol w/o authentication/authorization enforcement"
               (is (thrown-cause? NotAliveException
                            (.activate nimbus_client "topo-name"))))
      (.close client))))
  
(deftest test-noop-authorization-w-simple-transport 
  (with-test-cluster [6628 nil 
                "backtype.storm.security.auth.authorizer.NoopAuthorizer" 
                "backtype.storm.security.auth.SimpleTransportPlugin"]
    (let [storm-conf (merge (read-storm-config)
                             {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                              STORM-NIMBUS-RETRY-TIMES 0})
          client (NimbusClient. storm-conf "localhost" 6628 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Positive authorization) Authorization plugin should accept client request"
               (is (thrown-cause? NotAliveException
                            (.activate nimbus_client "topo-name"))))
      (.close client))))

(deftest test-deny-authorization-w-simple-transport 
  (with-test-cluster [6629 nil
                "backtype.storm.security.auth.authorizer.DenyAuthorizer" 
                "backtype.storm.security.auth.SimpleTransportPlugin"]
    (let [storm-conf (merge (read-storm-config)
                             {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                             Config/NIMBUS_HOST "localhost"
                             Config/NIMBUS_THRIFT_PORT 6629
                             STORM-NIMBUS-RETRY-TIMES 0})
          client (NimbusClient/getConfiguredClient storm-conf)
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
      (.close client))))

(deftest test-noop-authorization-w-sasl-digest 
  (with-test-cluster [6630
                "test/clj/backtype/storm/security/auth/jaas_digest.conf" 
                "backtype.storm.security.auth.authorizer.NoopAuthorizer" 
                "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                             Config/NIMBUS_HOST "localhost"
                             Config/NIMBUS_THRIFT_PORT 6630
                             STORM-NIMBUS-RETRY-TIMES 0})
          client (NimbusClient/getConfiguredClient storm-conf)
          nimbus_client (.getClient client)]
      (testing "(Positive authorization) Authorization plugin should accept client request"
               (is (thrown-cause? NotAliveException
                            (.activate nimbus_client "topo-name"))))
      (.close client))))

(deftest test-deny-authorization-w-sasl-digest 
  (with-test-cluster [6631
                "test/clj/backtype/storm/security/auth/jaas_digest.conf" 
                "backtype.storm.security.auth.authorizer.DenyAuthorizer" 
                "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                             Config/NIMBUS_HOST "localhost"
                             Config/NIMBUS_THRIFT_PORT 6631
                             STORM-NIMBUS-RETRY-TIMES 0})
          client (NimbusClient/getConfiguredClient storm-conf)
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
      (.close client))))

