(ns backtype.storm.security.auth.auth-test
  (:use [clojure test])
  (:require [backtype.storm.daemon [nimbus :as nimbus]])
  (:import [org.apache.thrift7 TException])
  (:import [org.apache.thrift7.transport TTransportException])
  (:import [java.nio ByteBuffer])
  (:import [backtype.storm Config])
  (:import [backtype.storm.utils NimbusClient])
  (:import [backtype.storm.security.auth AuthUtils ThriftServer ThriftClient 
                                         ReqContext])
  (:use [backtype.storm bootstrap util])
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap testing])
  (:import [backtype.storm.generated Nimbus Nimbus$Client])
  )

(bootstrap)

(def nimbus-timeout (Integer. 30))

(defn nimbus-data [storm-conf inimbus]
  (let [forced-scheduler (.getForcedScheduler inimbus)]
    {:conf storm-conf
     :inimbus inimbus
     :authorization-handler (mk-authorization-handler (storm-conf NIMBUS-AUTHORIZER) storm-conf)
     :submitted-count (atom 0)
     :storm-cluster-state nil
     :submit-lock (Object.)
     :heartbeats-cache (atom {})
     :downloaders nil
     :uploaders nil
     :uptime (uptime-computer)
     :validator nil
     :timer nil
     :scheduler nil
     }))

(defn check-authorization! [nimbus storm-name storm-conf operation]
  (let [aclHandler (:authorization-handler nimbus)]
    (log-debug "check-authorization with handler: " aclHandler)
    (if aclHandler
        (if-not (.permit aclHandler 
                  (ReqContext/context) 
                  operation 
                  (if storm-conf storm-conf {TOPOLOGY-NAME storm-name}))
          (throw (RuntimeException. (str operation " on topology " storm-name " is not authorized")))
          ))))

(defn dummy-service-handler [conf inimbus]
  (let [nimbus (nimbus-data conf inimbus)]
    (reify Nimbus$Iface
      (^void submitTopologyWithOpts [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology
                                     ^SubmitOptions submitOptions]
        (check-authorization! nimbus storm-name nil "submitTopology"))
      
      (^void killTopology [this ^String storm-name]
        (check-authorization! nimbus storm-name nil "killTopology"))
      
      (^void killTopologyWithOpts [this ^String storm-name ^KillOptions options]
        (check-authorization! nimbus storm-name nil "killTopology"))

      (^void rebalance [this ^String storm-name ^RebalanceOptions options]
        (check-authorization! nimbus storm-name nil "rebalance"))

      (activate [this storm-name]
        (check-authorization! nimbus storm-name nil "activate"))

      (deactivate [this storm-name]
        (check-authorization! nimbus storm-name nil "deactivate"))

      (beginFileUpload [this])

      (^void uploadChunk [this ^String location ^ByteBuffer chunk])

      (^void finishFileUpload [this ^String location])

      (^String beginFileDownload [this ^String file])

      (^ByteBuffer downloadChunk [this ^String id])

      (^String getNimbusConf [this])

      (^String getTopologyConf [this ^String id])

      (^StormTopology getTopology [this ^String id])

      (^StormTopology getUserTopology [this ^String id])

      (^ClusterSummary getClusterInfo [this])
      
      (^TopologyInfo getTopologyInfo [this ^String storm-id]))))

(defn launch-server [server-port login-cfg aznClass transportPluginClass] 
  (let [conf1 (merge (read-storm-config)
                    {NIMBUS-AUTHORIZER aznClass 
                     NIMBUS-HOST "localhost"
                     NIMBUS-THRIFT-PORT server-port
                     STORM-THRIFT-TRANSPORT-PLUGIN transportPluginClass})
        conf (if login-cfg (merge conf1 {"java.security.auth.login.config" login-cfg}) conf1)
        nimbus (nimbus/standalone-nimbus)
        service-handler (dummy-service-handler conf nimbus)
        server (ThriftServer. conf (Nimbus$Processor. service-handler) (int (conf NIMBUS-THRIFT-PORT)))]
    (.start (Thread. #(.serve server)))
    (wait-for-condition #(.isServing server))
    server ))

(defmacro with-server [args & body]
  `(let [server# (launch-server ~@args)]
     (try
       ~@body
       (catch Throwable t#
         (log-error t# "Error in cluster")
         (throw t#))
       (finally (.stop server#)))))

(deftest Simple-authentication-test 
  (with-server [6627 nil nil "backtype.storm.security.auth.SimpleTransportPlugin"]
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"})
          client (NimbusClient. storm-conf "localhost" 6627 nimbus-timeout)
          nimbus_client (.getClient client)]
      (.activate nimbus_client "security_auth_test_topology")
      (.close client))

    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"})]
      (testing "(Negative authentication) Server: Simple vs. Client: Digest"
               (is (thrown-cause?  java.net.SocketTimeoutException
                             (NimbusClient. storm-conf "localhost" 6627 nimbus-timeout)))))))
  
(deftest positive-authorization-test 
  (with-server [6627 nil 
                "backtype.storm.security.auth.authorizer.NoopAuthorizer" 
                "backtype.storm.security.auth.SimpleTransportPlugin"]
    (let [storm-conf (merge (read-storm-config)
                             {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"})
          client (NimbusClient. storm-conf "localhost" 6627 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Positive authorization) Authorization plugin should accept client request"
               (.activate nimbus_client "security_auth_test_topology"))
      (.close client))))

(deftest deny-authorization-test 
  (with-server [6627 nil
                "backtype.storm.security.auth.authorizer.DenyAuthorizer" 
                "backtype.storm.security.auth.SimpleTransportPlugin"]
    (let [storm-conf (merge (read-storm-config)
                             {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                             Config/NIMBUS_HOST "localhost"
                             Config/NIMBUS_THRIFT_PORT 6627
                             Config/NIMBUS_TASK_TIMEOUT_SECS nimbus-timeout})
          client (NimbusClient/getConfiguredClient storm-conf)
          nimbus_client (.getClient client)]
      (testing "(Negative authorization) Authorization plugin should reject client request"
               (is (thrown? TTransportException
                            (.activate nimbus_client "security_auth_test_topology"))))
               (.close client))))

(deftest digest-authentication-test
  (with-server [6627  
                "test/clj/backtype/storm/security/auth/jaas_digest.conf" 
                nil
                "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"]
    (let [storm-conf (merge (read-storm-config)
                             {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                              "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"})
          client (NimbusClient. storm-conf "localhost" 6627 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Positive authentication) valid digest authentication"
               (.activate nimbus_client "security_auth_test_topology"))
      (.close client))
    
    (let [storm-conf (merge (read-storm-config)
                             {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"})
          client (NimbusClient. storm-conf "localhost" 6627 nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Negative authentication) Server: Digest vs. Client: Simple"
               (is (thrown-cause? java.net.SocketTimeoutException
                                  (.activate nimbus_client "security_auth_test_topology"))))
      (.close client))
      
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest_bad_password.conf"})]
      (testing "(Negative authentication) Invalid  password"
               (is (thrown? TTransportException
                            (NimbusClient. storm-conf "localhost" 6627 nimbus-timeout)))))
      
    (let [storm-conf (merge (read-storm-config)
                             {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                              "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest_unknown_user.conf"})]
      (testing "(Negative authentication) Unknown user"
               (is (thrown? TTransportException 
                            (NimbusClient. storm-conf "localhost" 6627 nimbus-timeout)))))

    (let [storm-conf (merge (read-storm-config)
                             {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                              "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/nonexistent.conf"})]
      (testing "(Negative authentication) nonexistent configuration file"
               (is (thrown? RuntimeException 
                            (NimbusClient. storm-conf "localhost" 6627 nimbus-timeout)))))
    
    (let [storm-conf (merge (read-storm-config)
                            {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest_missing_client.conf"})]
      (testing "(Negative authentication) Missing client"
               (is (thrown-cause? java.io.IOException
                                  (NimbusClient. storm-conf "localhost" 6627 nimbus-timeout)))))))

        
(deftest test-GetTransportPlugin-throws-RuntimeException
  (let [conf (merge (read-storm-config)
                    {Config/STORM_THRIFT_TRANSPORT_PLUGIN "null.invalid"})]
    (is (thrown? RuntimeException (AuthUtils/GetTransportPlugin conf nil nil)))))
