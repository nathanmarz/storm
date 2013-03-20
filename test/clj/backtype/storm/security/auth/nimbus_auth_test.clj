(ns backtype.storm.security.auth.nimbus-auth-test
  (:use [clojure test])
  (:require [backtype.storm [testing :as testing]])
  (:require [backtype.storm.daemon [nimbus :as nimbus]])
  (:require [backtype.storm [zookeeper :as zk]])
  (:import [org.apache.thrift7 TException])
  (:import [org.apache.thrift7.transport TTransportException])
  (:import [java.nio ByteBuffer])
  (:import [backtype.storm Config])
  (:import [backtype.storm.utils NimbusClient])
  (:import [backtype.storm.generated NotAliveException])
  (:import [backtype.storm.security.auth AuthUtils ThriftServer ThriftClient 
                                         ReqContext])
  (:use [backtype.storm bootstrap cluster util])
  (:use [backtype.storm.daemon common nimbus])
  (:use [backtype.storm bootstrap])
  (:import [backtype.storm.generated Nimbus Nimbus$Client 
            AuthorizationException SubmitOptions TopologyInitialStatus KillOptions])
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
                                     nimbus-port)]
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
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"})
          client (NimbusClient. storm-conf "localhost" 6627 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Positive authorization) Simple protocol w/o authentication/authorization enforcement"
               (is (thrown? NotAliveException
                            (.activate nimbus_client "nimbus_auth_topology"))))
      (.close client))))
  
(deftest test-noop-authorization-w-simple-transport 
  (with-test-cluster [6627 nil 
                "backtype.storm.security.auth.authorizer.NoopAuthorizer" 
                "backtype.storm.security.auth.SimpleTransportPlugin"]
    (let [storm-conf (merge (read-storm-config)
                             {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"})
          client (NimbusClient. storm-conf "localhost" 6627 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Positive authorization) Authorization plugin should accept client request"
               (is (thrown? NotAliveException
                            (.activate nimbus_client "nimbus_auth_topology"))))
      (.close client))))

(deftest test-deny-authorization-w-simple-transport 
  (with-test-cluster [6627 nil
                "backtype.storm.security.auth.authorizer.DenyAuthorizer" 
                "backtype.storm.security.auth.SimpleTransportPlugin"]
    (let [storm-conf (merge (read-storm-config)
                             {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                             Config/NIMBUS_HOST "localhost"
                             Config/NIMBUS_THRIFT_PORT 6627})
          client (NimbusClient/getConfiguredClient storm-conf)
          nimbus_client (.getClient client)
          topologyInitialStatus (TopologyInitialStatus/findByValue 2)
          submitOptions (SubmitOptions. topologyInitialStatus)]
      (is (thrown? AuthorizationException (.submitTopology nimbus_client  "nimbus_auth_topology" nil nil nil))) 
      (is (thrown? AuthorizationException (.submitTopologyWithOpts nimbus_client  "nimbus_auth_topology" nil nil nil submitOptions)))
      (is (thrown? AuthorizationException (.killTopology nimbus_client "nimbus_auth_topology")))
      (is (thrown? AuthorizationException (.killTopologyWithOpts nimbus_client "nimbus_auth_topology" (KillOptions.))))
      (is (thrown? AuthorizationException (.activate nimbus_client "nimbus_auth_topology")))
      (is (thrown? AuthorizationException (.deactivate nimbus_client "nimbus_auth_topology")))
      (is (thrown? AuthorizationException (.rebalance nimbus_client "nimbus_auth_topology" nil)))
      (is (thrown? AuthorizationException (.beginFileUpload nimbus_client)))
      (is (thrown? AuthorizationException (.uploadChunk nimbus_client nil nil)))
      (is (thrown? AuthorizationException (.finishFileUpload nimbus_client nil)))
      (is (thrown? AuthorizationException (.beginFileDownload nimbus_client nil)))
      (is (thrown? AuthorizationException (.downloadChunk nimbus_client nil)))
      (is (thrown? AuthorizationException (.getNimbusConf nimbus_client)))
      (is (thrown? AuthorizationException (.getClusterInfo nimbus_client)))
      (is (thrown? AuthorizationException (.getTopologyInfo nimbus_client "nimbus_auth_topology")))
      (is (thrown? AuthorizationException (.getTopologyConf nimbus_client "nimbus_auth_topology")))
      (is (thrown? AuthorizationException (.getTopology nimbus_client "nimbus_auth_topology")))
      (is (thrown? AuthorizationException (.getUserTopology nimbus_client "nimbus_auth_topology")))
      (.close client))))

(deftest test-noop-authorization-w-sasl-digest 
  (with-test-cluster [6627 
                "test/clj/backtype/storm/security/auth/jaas_digest.conf" 
                "backtype.storm.security.auth.authorizer.NoopAuthorizer" 
                "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                             Config/NIMBUS_HOST "localhost"
                             Config/NIMBUS_THRIFT_PORT 6627})
          client (NimbusClient/getConfiguredClient storm-conf)
          nimbus_client (.getClient client)]
      (testing "(Positive authorization) Authorization plugin should accept client request"
               (is (thrown? NotAliveException
                            (.activate nimbus_client "nimbus_auth_topology"))))
      (.close client))))

(deftest test-deny-authorization-w-sasl-digest 
  (with-test-cluster [6627 
                "test/clj/backtype/storm/security/auth/jaas_digest.conf" 
                "backtype.storm.security.auth.authorizer.DenyAuthorizer" 
                "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                             Config/NIMBUS_HOST "localhost"
                             Config/NIMBUS_THRIFT_PORT 6627})
          client (NimbusClient/getConfiguredClient storm-conf)
          nimbus_client (.getClient client)
          topologyInitialStatus (TopologyInitialStatus/findByValue 2)
          submitOptions (SubmitOptions. topologyInitialStatus)]
      (is (thrown? AuthorizationException (.submitTopology nimbus_client  "nimbus_auth_topology" nil nil nil))) 
      (is (thrown? AuthorizationException (.submitTopologyWithOpts nimbus_client  "nimbus_auth_topology" nil nil nil submitOptions)))
      (is (thrown? AuthorizationException (.killTopology nimbus_client "nimbus_auth_topology")))
      (is (thrown? AuthorizationException (.killTopologyWithOpts nimbus_client "nimbus_auth_topology" (KillOptions.))))
      (is (thrown? AuthorizationException (.activate nimbus_client "nimbus_auth_topology")))
      (is (thrown? AuthorizationException (.deactivate nimbus_client "nimbus_auth_topology")))
      (is (thrown? AuthorizationException (.rebalance nimbus_client "nimbus_auth_topology" nil)))
      (is (thrown? AuthorizationException (.beginFileUpload nimbus_client)))
      (is (thrown? AuthorizationException (.uploadChunk nimbus_client nil nil)))
      (is (thrown? AuthorizationException (.finishFileUpload nimbus_client nil)))
      (is (thrown? AuthorizationException (.beginFileDownload nimbus_client nil)))
      (is (thrown? AuthorizationException (.downloadChunk nimbus_client nil)))
      (is (thrown? AuthorizationException (.getNimbusConf nimbus_client)))
      (is (thrown? AuthorizationException (.getClusterInfo nimbus_client)))
      (is (thrown? AuthorizationException (.getTopologyInfo nimbus_client "nimbus_auth_topology")))
      (is (thrown? AuthorizationException (.getTopologyConf nimbus_client "nimbus_auth_topology")))
      (is (thrown? AuthorizationException (.getTopology nimbus_client "nimbus_auth_topology")))
      (is (thrown? AuthorizationException (.getUserTopology nimbus_client "nimbus_auth_topology")))
      (.close client))))

