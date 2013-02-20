(ns backtype.storm.security.auth.auth-test
  (:use [clojure test])
  (:require [backtype.storm.daemon [nimbus :as nimbus]])
  (:import [org.apache.thrift7 TException])
  (:import [org.apache.thrift7.transport TTransportException])
  (:import [java.nio ByteBuffer])
  (:import [backtype.storm.utils NimbusClient])
  (:import [backtype.storm.security.auth ThriftServer ThriftClient ReqContext ReqContext$OperationType])
  (:use [backtype.storm bootstrap util])
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap testing])
  (:import [backtype.storm.generated Nimbus Nimbus$Client])
  )

(bootstrap)

(def nimbus-timeout (Integer. 30))

(defn mk-authorization-handler [conf]
  (let [klassname (conf NIMBUS-AUTHORIZATION-CLASSNAME) 
        aznClass (if klassname (Class/forName klassname))
        aznHandler (if aznClass (.newInstance aznClass))] 
    (log-debug "authorization class name:" klassname
               " class:" aznClass
               " handler:" aznHandler)
    aznHandler
    ))

(defn nimbus-data [conf inimbus]
  (let [forced-scheduler (.getForcedScheduler inimbus)]
    {:conf conf
     :inimbus inimbus
     :authorization-handler (mk-authorization-handler conf)
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

(defn update-req-context! [nimbus storm-name storm-conf operation]
  (let [req (ReqContext/context)]
    (.setOperation req operation)
    (if storm-conf (.setTopologyConf req storm-conf) 
      (let [topologyConf { TOPOLOGY-NAME storm-name} ]
        (.setTopologyConf req topologyConf)))
    req))

(defn check-authorization! [nimbus storm-name storm-conf operation]
  (let [aclHandler (:authorization-handler nimbus)]
    (log-debug "check-authorization with handler: " aclHandler)
    (if aclHandler
      (let [req (update-req-context! nimbus storm-name storm-conf operation)]
        (if-not (.permit aclHandler req)
          (throw (RuntimeException. (str operation " on topology " storm-name " is not authorized")))
          )))))

(defn dummy-service-handler [conf inimbus]
  (let [nimbus (nimbus-data conf inimbus)]
    (reify Nimbus$Iface
      (^void submitTopologyWithOpts [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology
                                     ^SubmitOptions submitOptions]
        (check-authorization! nimbus storm-name nil (ReqContext$OperationType/SUBMIT_TOPOLOGY)))
      
      (^void killTopology [this ^String storm-name]
        (check-authorization! nimbus storm-name nil (ReqContext$OperationType/KILL_TOPOLOGY)))
      
      (^void killTopologyWithOpts [this ^String storm-name ^KillOptions options]
        (check-authorization! nimbus storm-name nil (ReqContext$OperationType/KILL_TOPOLOGY)))

      (^void rebalance [this ^String storm-name ^RebalanceOptions options]
        (check-authorization! nimbus storm-name nil (ReqContext$OperationType/REBALANCE_TOPOLOGY)))

      (activate [this storm-name]
        (check-authorization! nimbus storm-name nil (ReqContext$OperationType/ACTIVATE_TOPOLOGY)))

      (deactivate [this storm-name]
        (check-authorization! nimbus storm-name nil (ReqContext$OperationType/DEACTIVATE_TOPOLOGY)))

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

(defn launch-test-server [server-port login-cfg aznClass transportPluginClass transportPluginJAR] 
  (System/setProperty "java.security.auth.login.config" login-cfg)
  (let [conf (merge (read-storm-config)
                    {NIMBUS-AUTHORIZATION-CLASSNAME aznClass 
                     NIMBUS-HOST "localhost"
                     NIMBUS-THRIFT-PORT server-port
                     STORM-THRIFT-TRANSPORT-PLUGIN-CLASS transportPluginClass
                     STORM-THRIFT-TRANSPORT-PLUGIN-JAR transportPluginJAR})
        nimbus (nimbus/standalone-nimbus)
        service-handler (dummy-service-handler conf nimbus)
        server (ThriftServer. conf (Nimbus$Processor. service-handler) (int (conf NIMBUS-THRIFT-PORT)))]
    (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.stop server))))
    (.serve server)))

(defn launch-server-w-wait [server-port ms login-cfg aznClass transportPluginClass transportPluginJAR]
  (.start (Thread. #(launch-test-server server-port login-cfg aznClass transportPluginClass transportPluginJAR)))
  (Thread/sleep ms))

(deftest Simple-authentication-test 
  (launch-server-w-wait 6627 1000 "" nil "backtype.storm.security.auth.SimpleTransportPlugin" nil)

  (log-message "(Positive authentication) Server and Client with simple transport, no authentication")
  (let [storm-conf (merge (read-storm-config)
                          {STORM-THRIFT-TRANSPORT-PLUGIN-CLASS "backtype.storm.security.auth.SimpleTransportPlugin"})
        client (NimbusClient. storm-conf "localhost" 6627 nimbus-timeout)
        nimbus_client (.getClient client)]
    (.activate nimbus_client "security_auth_test_topology")
    (.close client))

  (log-message "(Negative authentication) Server: Simple vs. Client: Digest")
  (System/setProperty "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf")
  (log-message "java.security.auth.login.config: " (System/getProperty "java.security.auth.login.config"))
  (let [storm-conf (merge (read-storm-config)
                          {STORM-THRIFT-TRANSPORT-PLUGIN-CLASS "backtype.storm.security.auth.DigestSaslTransportPlugin"})]
    (is (= "java.net.SocketTimeoutException: Read timed out" 
           (try (NimbusClient. storm-conf "localhost" 6627 nimbus-timeout)
             nil
             (catch TTransportException ex (.getMessage ex)))))))

(deftest positive-authorization-test 
  (launch-server-w-wait 6628 1000 "" 
                        "backtype.storm.security.auth.NoopAuthorizer" 
                        "backtype.storm.security.auth.SimpleTransportPlugin" 
                        nil)
  (let [storm-conf (merge (read-storm-config)
                           {STORM-THRIFT-TRANSPORT-PLUGIN-CLASS "backtype.storm.security.auth.SimpleTransportPlugin"})
        client (NimbusClient. storm-conf "localhost" 6628 nimbus-timeout)
        nimbus_client (.getClient client)]
    (log-message "(Positive authorization) Authorization plugin should accept client request")
    (.activate nimbus_client "security_auth_test_topology")
    (.close client)))

(deftest deny-authorization-test 
  (launch-server-w-wait 6629 1000 "" 
                        "backtype.storm.security.auth.DenyAuthorizer" 
                        "backtype.storm.security.auth.SimpleTransportPlugin" 
                        nil)
  (let [storm-conf (merge (read-storm-config)
                           {STORM-THRIFT-TRANSPORT-PLUGIN-CLASS "backtype.storm.security.auth.SimpleTransportPlugin"})
        client (NimbusClient. storm-conf "localhost" 6629 nimbus-timeout)
        nimbus_client (.getClient client)]
    (log-message "(Negative authorization) Authorization plugin should reject client request")
    (is (thrown? TTransportException
                 (.activate nimbus_client "security_auth_test_topology")))
    (.close client)))

(deftest digest-authentication-test
  (launch-server-w-wait 6630 2000 
                        "test/clj/backtype/storm/security/auth/jaas_digest.conf" 
                        nil
                        "backtype.storm.security.auth.DigestSaslTransportPlugin" 
                        nil)
  (log-message "(Positive authentication) valid digest authentication")
  (System/setProperty "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf")
  (let [storm-conf (merge (read-storm-config)
                           {STORM-THRIFT-TRANSPORT-PLUGIN-CLASS "backtype.storm.security.auth.DigestSaslTransportPlugin"})
        client (NimbusClient. storm-conf "localhost" 6630 nimbus-timeout)
        nimbus_client (.getClient client)]
    (.activate nimbus_client "security_auth_test_topology")
    (.close client))
  
  (log-message "(Negative authentication) Server: Digest vs. Client: Simple")
  (System/setProperty "java.security.auth.login.config" "")
  (let [storm-conf (merge (read-storm-config)
                           {STORM-THRIFT-TRANSPORT-PLUGIN-CLASS "backtype.storm.security.auth.SimpleTransportPlugin"})
        client (NimbusClient. storm-conf "localhost" 6630 nimbus-timeout)
        nimbus_client (.getClient client)]
    (is (thrown? TTransportException
                 (.activate nimbus_client "security_auth_test_topology")))
    (.close client))
    
  (log-message "(Negative authentication) Invalid  password")
  (System/setProperty "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest_bad_password.conf")
  (let [storm-conf (merge (read-storm-config)
                           {STORM-THRIFT-TRANSPORT-PLUGIN-CLASS "backtype.storm.security.auth.DigestSaslTransportPlugin"})]
    (is (= "Peer indicated failure: DIGEST-MD5: digest response format violation. Mismatched response." 
           (try (NimbusClient. storm-conf "localhost" 6630 nimbus-timeout)
             nil
             (catch TTransportException ex (.getMessage ex))))))
    
  (log-message "(Negative authentication) Unknown user")
  (System/setProperty "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest_unknown_user.conf")
  (let [storm-conf (merge (read-storm-config)
                           {STORM-THRIFT-TRANSPORT-PLUGIN-CLASS "backtype.storm.security.auth.DigestSaslTransportPlugin"})]
    (is (= "Peer indicated failure: DIGEST-MD5: cannot acquire password for unknown_user in realm : localhost" 
           (try (NimbusClient. storm-conf "localhost" 6630 nimbus-timeout)
           nil
           (catch TTransportException ex (.getMessage ex)))))))

;
;
;(deftest anonymous-authentication-test 
;  (launch-server-w-wait 6625 1000 "" nil "backtype.storm.security.auth.AnonymousSaslTransportPlugin"
;                        "../storm-auth-plugin/target/storm-auth-plugin-sample-1.0-SNAPSHOT.jar")
;
;  (log-message "(Positive authentication) Server and Client with anonymous authentication")
;  (let [storm-conf (merge (read-storm-config)
;                          {STORM-THRIFT-TRANSPORT-PLUGIN-CLASS "backtype.storm.security.auth.AnonymousSaslTransportPlugin"
;                           STORM-THRIFT-TRANSPORT-PLUGIN-JAR "../storm-auth-plugin/target/storm-auth-plugin-sample-1.0-SNAPSHOT.jar"})
;        client (NimbusClient. storm-conf "localhost" 6625 nimbus-timeout)
;        nimbus_client (.getClient client)]
;    (.activate nimbus_client "security_auth_test_topology")
;    (.close client))
;
; (log-message "(Negative authentication) Server: anonymous vs. Client: Digest")
;  (System/setProperty "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf")
;  (log-message "java.security.auth.login.config: " (System/getProperty "java.security.auth.login.config"))
;  (let [storm-conf (merge (read-storm-config)
;                          {STORM-THRIFT-TRANSPORT-PLUGIN-CLASS "backtype.storm.security.auth.DigestSaslTransportPlugin"})]
;    (is (= "Peer indicated failure: Unsupported mechanism type DIGEST-MD5" 
;           (try (NimbusClient. storm-conf "localhost" 6625 nimbus-timeout)
;             nil
;             (catch TTransportException ex (.getMessage ex)))))))
;
;(deftest anonymous-positive-authorization-test 
;  (launch-server-w-wait 6623 1000 "" 
;                        "backtype.storm.security.auth.NoopAuthorizer" 
;                        "backtype.storm.security.auth.AnonymousSaslTransportPlugin"
;                        "../storm-auth-plugin/target/storm-auth-plugin-sample-1.0-SNAPSHOT.jar")
;  (let [storm-conf (merge (read-storm-config)
;                           {STORM-THRIFT-TRANSPORT-PLUGIN-CLASS "backtype.storm.security.auth.AnonymousSaslTransportPlugin"
;                            STORM-THRIFT-TRANSPORT-PLUGIN-JAR "../storm-auth-plugin/target/storm-auth-plugin-sample-1.0-SNAPSHOT.jar"})
;        client (NimbusClient. storm-conf "localhost" 6623 nimbus-timeout)
;        nimbus_client (.getClient client)]
;    (log-message "(Positive authorization) Authorization plugin should accept client request")
;    (.activate nimbus_client "security_auth_test_topology")
;    (.close client)))
;
;(deftest anonymous-deny-authorization-test 
;  (launch-server-w-wait 6624 1000 "" 
;                        "backtype.storm.security.auth.DenyAuthorizer" 
;                        "backtype.storm.security.auth.AnonymousSaslTransportPlugin"
;                        "../storm-auth-plugin/target/storm-auth-plugin-sample-1.0-SNAPSHOT.jar")
;  (let [storm-conf (merge (read-storm-config)
;                           {STORM-THRIFT-TRANSPORT-PLUGIN-CLASS "backtype.storm.security.auth.AnonymousSaslTransportPlugin"
;                            STORM-THRIFT-TRANSPORT-PLUGIN-JAR "../storm-auth-plugin/target/storm-auth-plugin-sample-1.0-SNAPSHOT.jar"})
;        client (NimbusClient. storm-conf "localhost" 6624 nimbus-timeout)
;        nimbus_client (.getClient client)]
;    (log-message "(Negative authorization) Authorization plugin should reject client request")
;    (is (thrown? TTransportException
;                 (.activate nimbus_client "security_auth_test_topology")))
;    (.close client)))
