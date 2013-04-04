(ns backtype.storm.security.auth.nimbus-auth-test
  (:use [clojure test])
  (:require [backtype.storm [testing :as testing]])
  (:import [org.apache.thrift7 TException])
  (:import [org.apache.thrift7.transport TTransportException])
  (:import [backtype.storm Config])
  (:import [backtype.storm.utils NimbusClient])
  (:import [backtype.storm.generated NotAliveException])
  (:import [backtype.storm.security.auth AuthUtils ThriftServer ThriftClient 
                                         ReqContext])
  (:import [backtype.storm.testing TestWordSpout])
  (:use [backtype.storm bootstrap cluster util])
  (:use [backtype.storm.daemon common nimbus])
  (:use [backtype.storm bootstrap])
  (:import [backtype.storm.generated Nimbus Nimbus$Client 
            AuthorizationException SubmitOptions TopologyInitialStatus KillOptions])
  )

(bootstrap)

(def nimbus-timeout (Integer. 30))

(defn launch-test-cluster [nimbus-port login-cfg aznClass transportPluginClass allowed_ops] 
  (let [conf {NIMBUS-AUTHORIZER aznClass 
              NIMBUS-THRIFT-PORT nimbus-port
              STORM-THRIFT-TRANSPORT-PLUGIN transportPluginClass }
        conf (if login-cfg (merge conf {"java.security.auth.login.config" login-cfg}) conf)
        conf (if allowed_ops (merge conf {"storm.denyauthorizer.exceptions" allowed_ops}) conf) 
        cluster-map (testing/mk-local-storm-cluster :supervisors 0
                                            :ports-per-supervisor 0
                                            :daemon-conf conf)
        nimbus-server (ThriftServer. (:daemon-conf cluster-map)
                                     (Nimbus$Processor. (:nimbus cluster-map)) 
                                     nimbus-port)]
    (.start (Thread. #(.serve nimbus-server)))
    (wait-for-condition #(.isServing nimbus-server))
    [cluster-map nimbus-server]))

(defmacro with-test-cluster [args & body]
  `(let [[cluster-map#  nimbus-server#] (launch-test-cluster  ~@args)]
     (try
       ~@body
       (catch Throwable t#
         (log-error t# "Error in cluster")
         (throw t#)
         )
       (finally
         (testing/kill-local-storm-cluster cluster-map#)
         (.stop nimbus-server#)))))     

(deftest test-simple-authentication 
  (with-test-cluster [6627 nil nil "backtype.storm.security.auth.SimpleTransportPlugin" nil]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                             Config/NIMBUS_HOST "localhost"
                             Config/NIMBUS_THRIFT_PORT 6627})
          storm-conf-json (to-json storm-conf)
          topology (thrift/mk-topology {"1" (thrift/mk-spout-spec (TestWordSpout. true))} {})
          client (NimbusClient/getConfiguredClient storm-conf)
          nimbus_client (.getClient client)]
      (.submitTopology nimbus_client  "test-deny-nimbus-auth" nil storm-conf-json topology) 
      (.close client))))

(deftest test-noop-authorization-w-simple-transport 
  (with-test-cluster [6627 nil 
                "backtype.storm.security.auth.authorizer.NoopAuthorizer" 
                "backtype.storm.security.auth.SimpleTransportPlugin" nil]
    (let [storm-conf (merge (read-storm-config)
                             {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                              Config/NIMBUS_HOST "localhost"
                             Config/NIMBUS_THRIFT_PORT 6627})
          storm-conf-json (to-json storm-conf)
          topology (thrift/mk-topology {"1" (thrift/mk-spout-spec (TestWordSpout. true))} {})
          client (NimbusClient/getConfiguredClient storm-conf)
          nimbus_client (.getClient client)]
      (.submitTopology nimbus_client  "test-deny-nimbus-auth" nil storm-conf-json topology) 
      (.close client))))

(deftest test-deny-authorization-w-simple-transport 
  (with-test-cluster [6627 nil
                "backtype.storm.security.auth.authorizer.DenyAuthorizer" 
                "backtype.storm.security.auth.SimpleTransportPlugin" nil]
    (let [storm-conf (merge (read-storm-config)
                             {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                             Config/NIMBUS_HOST "localhost"
                             Config/NIMBUS_THRIFT_PORT 6627})
          topology (thrift/mk-topology {"1" (thrift/mk-spout-spec (TestWordSpout. true))} {})
          client (NimbusClient/getConfiguredClient storm-conf)
          storm-conf-json (to-json storm-conf)
          nimbus_client (.getClient client)
          submitOptions (SubmitOptions. (TopologyInitialStatus/INACTIVE))]
      (is (thrown? AuthorizationException (.submitTopology nimbus_client  "test-deny-nimbus-auth" nil storm-conf-json topology))) 
      (is (thrown? AuthorizationException (.submitTopologyWithOpts nimbus_client  "test-deny-nimbus-auth" nil storm-conf-json topology submitOptions)))
      (is (thrown? AuthorizationException (.beginFileUpload nimbus_client)))
      (is (thrown? AuthorizationException (.uploadChunk nimbus_client nil nil)))
      (is (thrown? AuthorizationException (.finishFileUpload nimbus_client nil)))
      (is (thrown? AuthorizationException (.beginFileDownload nimbus_client nil)))
      (is (thrown? AuthorizationException (.downloadChunk nimbus_client nil)))
      (is (thrown? AuthorizationException (.getNimbusConf nimbus_client)))
      (is (thrown? AuthorizationException (.getClusterInfo nimbus_client)))
      (.close client))))

(deftest test-access-existing-topology-w-simple-transport 
  (let [[cluster-map  nimbus-server] (launch-test-cluster 6627 nil
                "backtype.storm.security.auth.authorizer.DenyAuthorizer" 
                "backtype.storm.security.auth.SimpleTransportPlugin"
                "submitTopology")
        storm-conf (merge (read-storm-config)
                          {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                           Config/NIMBUS_HOST "localhost"
                           Config/NIMBUS_THRIFT_PORT 6627})
        client (NimbusClient/getConfiguredClient storm-conf)
        storm-conf-json (to-json storm-conf)
        nimbus_client (.getClient client)
        storm-name  "test-access-existing-topology"
        topology (thrift/mk-topology {"1" (thrift/mk-spout-spec (TestWordSpout. true))} {} )
        _ (.submitTopology nimbus_client  storm-name  nil storm-conf-json topology)
        storm-id (get-storm-id  (:storm-cluster-state cluster-map) storm-name)]
    (is (thrown? AuthorizationException (.killTopology nimbus_client storm-name)))
    (is (thrown? AuthorizationException (.killTopologyWithOpts nimbus_client storm-name (KillOptions.))))
    (is (thrown? AuthorizationException (.activate nimbus_client storm-name)))
    (is (thrown? AuthorizationException (.deactivate nimbus_client storm-name)))
    (is (thrown? AuthorizationException (.rebalance nimbus_client storm-name nil)))
    (is (thrown? AuthorizationException (.getTopologyInfo nimbus_client storm-id)))
    (is (thrown? AuthorizationException (.getTopologyConf nimbus_client storm-id)))
    (is (thrown? AuthorizationException (.getTopology nimbus_client storm-id)))
    (is (thrown? AuthorizationException (.getUserTopology nimbus_client storm-id)))
    (.close client)
    (testing/kill-local-storm-cluster cluster-map)
    (.stop nimbus-server)))

(deftest test-noop-authorization-w-sasl-digest 
  (with-test-cluster [6627 
                "test/clj/backtype/storm/security/auth/jaas_digest.conf" 
                "backtype.storm.security.auth.authorizer.NoopAuthorizer" 
                "backtype.storm.security.auth.digest.DigestSaslTransportPlugin" nil]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                             Config/NIMBUS_HOST "localhost"
                             Config/NIMBUS_THRIFT_PORT 6627})
          storm-conf-json (to-json storm-conf)
          topology (thrift/mk-topology {"1" (thrift/mk-spout-spec (TestWordSpout. true))} {})
          client (NimbusClient/getConfiguredClient storm-conf)
          nimbus_client (.getClient client)]
      (.submitTopology nimbus_client  "test-noop-sasl-topology" nil storm-conf-json topology)
      (.close client))))

(deftest test-deny-authorization-w-sasl-digest 
  (with-test-cluster [6627 
                "test/clj/backtype/storm/security/auth/jaas_digest.conf" 
                "backtype.storm.security.auth.authorizer.DenyAuthorizer" 
                "backtype.storm.security.auth.digest.DigestSaslTransportPlugin" nil]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                             Config/NIMBUS_HOST "localhost"
                             Config/NIMBUS_THRIFT_PORT 6627})
          storm-conf-json (to-json storm-conf)
          topology (thrift/mk-topology {"1" (thrift/mk-spout-spec (TestWordSpout. true))} {})
          client (NimbusClient/getConfiguredClient storm-conf)
          nimbus_client (.getClient client)
          submitOptions (SubmitOptions. (TopologyInitialStatus/INACTIVE))]
      (is (thrown? AuthorizationException (.submitTopology nimbus_client  "test-deny-sasl-topology" nil storm-conf-json topology))) 
      (is (thrown? AuthorizationException (.submitTopologyWithOpts nimbus_client  "test-deny-sasl-topology" nil storm-conf-json topology submitOptions)))
      (is (thrown? AuthorizationException (.beginFileUpload nimbus_client)))
      (is (thrown? AuthorizationException (.uploadChunk nimbus_client nil nil)))
      (is (thrown? AuthorizationException (.finishFileUpload nimbus_client nil)))
      (is (thrown? AuthorizationException (.beginFileDownload nimbus_client nil)))
      (is (thrown? AuthorizationException (.downloadChunk nimbus_client nil)))
      (is (thrown? AuthorizationException (.getNimbusConf nimbus_client)))
      (is (thrown? AuthorizationException (.getClusterInfo nimbus_client)))
      (.close client))))

(deftest test-deny-access-existing-topology-w-sasl-digest 
  (let [[cluster-map  nimbus-server] (launch-test-cluster 6627
                "test/clj/backtype/storm/security/auth/jaas_digest.conf" 
                "backtype.storm.security.auth.authorizer.DenyAuthorizer" 
                "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                "submitTopology")
        storm-conf (merge (read-storm-config)
                          {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                           "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                           Config/NIMBUS_HOST "localhost"
                           Config/NIMBUS_THRIFT_PORT 6627
                           "storm.denyauthorizer.exceptions" "submitTopology"})
        client (NimbusClient/getConfiguredClient storm-conf)
        storm-conf-json (to-json storm-conf)
        nimbus_client (.getClient client)
        storm-name  "test-access-existing-sasl-topology"
        topology (thrift/mk-topology {"1" (thrift/mk-spout-spec (TestWordSpout. true))} {})
        _ (.submitTopology nimbus_client  storm-name  nil storm-conf-json topology)
        storm-id (get-storm-id  (:storm-cluster-state cluster-map) storm-name)]
    (is (thrown? AuthorizationException (.killTopology nimbus_client storm-name)))
    (is (thrown? AuthorizationException (.killTopologyWithOpts nimbus_client storm-name (KillOptions.))))
    (is (thrown? AuthorizationException (.activate nimbus_client storm-name)))
    (is (thrown? AuthorizationException (.deactivate nimbus_client storm-name)))
    (is (thrown? AuthorizationException (.rebalance nimbus_client storm-name nil)))
    (is (thrown? AuthorizationException (.getTopologyInfo nimbus_client storm-id)))
    (is (thrown? AuthorizationException (.getTopologyConf nimbus_client storm-id)))
    (is (thrown? AuthorizationException (.getTopology nimbus_client storm-id)))
    (is (thrown? AuthorizationException (.getUserTopology nimbus_client storm-id)))
    (.close client)
    (testing/kill-local-storm-cluster cluster-map)
    (.stop nimbus-server)))
