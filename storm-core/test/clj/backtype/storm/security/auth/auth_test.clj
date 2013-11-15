(ns backtype.storm.security.auth.auth-test
  (:use [clojure test])
  (:require [backtype.storm.daemon [nimbus :as nimbus]])
  (:import [org.apache.thrift7 TException])
  (:import [org.apache.thrift7.transport TTransportException])
  (:import [java.nio ByteBuffer])
  (:import [java.security Principal AccessController])
  (:import [javax.security.auth Subject])
  (:import [java.net InetAddress])
  (:import [backtype.storm Config])
  (:import [backtype.storm.generated AuthorizationException])
  (:import [backtype.storm.utils NimbusClient])
  (:import [backtype.storm.security.auth.authorizer SimpleWhitelistAuthorizer SimpleACLAuthorizer])
  (:import [backtype.storm.security.auth AuthUtils ThriftServer ThriftClient 
            ReqContext SimpleTransportPlugin])
  (:use [backtype.storm bootstrap util])
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap testing])
  (:import [backtype.storm.generated Nimbus Nimbus$Client]))

(bootstrap)

(defn mk-principal [name]
  (reify Principal
    (equals [this other]
      (= name (.getName other)))
    (getName [this] name)
    (toString [this] name)
    (hashCode [this] (.hashCode name))))

(defn mk-subject [name]
  (Subject. true #{(mk-principal name)} #{} #{}))

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

(defn dummy-service-handler 
  ([conf inimbus auth-context]
     (let [nimbus-d (nimbus-data conf inimbus)
           topo-conf (atom nil)]
       (reify Nimbus$Iface
         (^void submitTopologyWithOpts [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology
                                        ^SubmitOptions submitOptions]
           (if (not (nil? serializedConf)) (swap! topo-conf (fn [prev new] new) (from-json serializedConf)))
           (nimbus/check-authorization! nimbus-d storm-name @topo-conf "submitTopology" auth-context))
         
         (^void killTopology [this ^String storm-name]
           (nimbus/check-authorization! nimbus-d storm-name @topo-conf "killTopology" auth-context))
         
         (^void killTopologyWithOpts [this ^String storm-name ^KillOptions options]
           (nimbus/check-authorization! nimbus-d storm-name @topo-conf "killTopology" auth-context))
         
         (^void rebalance [this ^String storm-name ^RebalanceOptions options]
           (nimbus/check-authorization! nimbus-d storm-name @topo-conf "rebalance" auth-context))
         
         (activate [this storm-name]
           (nimbus/check-authorization! nimbus-d storm-name @topo-conf "activate" auth-context))
         
         (deactivate [this storm-name]
           (nimbus/check-authorization! nimbus-d storm-name @topo-conf "deactivate" auth-context))

         (beginFileUpload [this])
         
         (^void uploadChunk [this ^String location ^ByteBuffer chunk])
         
         (^void finishFileUpload [this ^String location])
         
         (^String beginFileDownload [this ^String file]
           (nimbus/check-authorization! nimbus-d nil nil "fileDownload" auth-context)
           "Done!")
         
         (^ByteBuffer downloadChunk [this ^String id])
         
         (^String getNimbusConf [this])
         
         (^String getTopologyConf [this ^String id])
         
         (^StormTopology getTopology [this ^String id])
         
         (^StormTopology getUserTopology [this ^String id])
         
         (^ClusterSummary getClusterInfo [this])
         
         (^TopologyInfo getTopologyInfo [this ^String storm-id]))))
  ([conf inimbus]
     (dummy-service-handler conf inimbus nil)))
     

(defn launch-server [server-port login-cfg aznClass transportPluginClass serverConf] 
  (let [conf1 (merge (read-storm-config)
                     {NIMBUS-AUTHORIZER aznClass 
                      NIMBUS-HOST "localhost"
                      NIMBUS-THRIFT-PORT server-port
                      STORM-THRIFT-TRANSPORT-PLUGIN transportPluginClass})
        conf2 (if login-cfg (merge conf1 {"java.security.auth.login.config" login-cfg}) conf1)
        conf (if serverConf (merge conf2 serverConf) conf2)
        nimbus (nimbus/standalone-nimbus)
        service-handler (dummy-service-handler conf nimbus)
        server (ThriftServer. 
                conf 
                (Nimbus$Processor. service-handler) 
                (int (conf NIMBUS-THRIFT-PORT))
                backtype.storm.Config$ThriftServerPurpose/NIMBUS)]
    (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.stop server))))
    (.start (Thread. #(.serve server)))
    (wait-for-condition #(.isServing server))
    server ))

(defmacro with-server [args & body]
  `(let [server# (launch-server ~@args)]
     ~@body
     (.stop server#)
     ))

(deftest Simple-authentication-test
  (let [a-port (available-port)]
    (with-server [a-port nil nil "backtype.storm.security.auth.SimpleTransportPlugin" nil]
      (let [storm-conf (merge (read-storm-config)
                              {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"})
            client (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)
            nimbus_client (.getClient client)]
        (.activate nimbus_client "security_auth_test_topology")
        (.close client))
      
      (let [storm-conf (merge (read-storm-config)
                              {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                               STORM-NIMBUS-RETRY-TIMES 0})]
        (testing "(Negative authentication) Server: Simple vs. Client: Digest"
          (is (thrown-cause?  java.net.SocketTimeoutException
                              (NimbusClient. storm-conf "localhost" a-port nimbus-timeout))))))))
  
(deftest negative-whitelist-authorization-test 
  (let [a-port (available-port)]
    (with-server [a-port nil
                  "backtype.storm.security.auth.authorizer.SimpleWhitelistAuthorizer" 
                  "backtype.storm.testing.SingleUserSimpleTransport" nil]
      (let [storm-conf (merge (read-storm-config)
                              {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.testing.SingleUserSimpleTransport"})
            client (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)
            nimbus_client (.getClient client)]
        (testing "(Negative authorization) Authorization plugin should reject client request"
          (is (thrown-cause? AuthorizationException
                             (.activate nimbus_client "security_auth_test_topology"))))
        (.close client)))))

(deftest positive-whitelist-authorization-test 
    (let [a-port (available-port)]
      (with-server [a-port nil
                    "backtype.storm.security.auth.authorizer.SimpleWhitelistAuthorizer" 
                    "backtype.storm.testing.SingleUserSimpleTransport" {SimpleWhitelistAuthorizer/WHITELIST_USERS_CONF ["user"]}]
        (let [storm-conf (merge (read-storm-config)
                                {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.testing.SingleUserSimpleTransport"})
              client (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)
              nimbus_client (.getClient client)]
          (testing "(Positive authorization) Authorization plugin should accept client request"
            (.activate nimbus_client "security_auth_test_topology"))
          (.close client)))))

(deftest simple-acl-user-auth-test
  (let [cluster-conf (merge (read-storm-config)
                       {NIMBUS-ADMINS ["admin"]
                        NIMBUS-SUPERVISOR-USERS ["supervisor"]})
        authorizer (SimpleACLAuthorizer. )
        admin-user (mk-subject "admin")
        supervisor-user (mk-subject "supervisor")
        user-a (mk-subject "user-a")
        user-b (mk-subject "user-b")]
  (.prepare authorizer cluster-conf) 
  (is (= true (.permit authorizer (ReqContext. user-a) "submitTopology" {})))
  (is (= true (.permit authorizer (ReqContext. user-b) "submitTopology" {})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "submitTopology" {})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "submitTopology" {})))

  (is (= true (.permit authorizer (ReqContext. user-a) "fileUpload" nil)))
  (is (= true (.permit authorizer (ReqContext. user-b) "fileUpload" nil)))
  (is (= true (.permit authorizer (ReqContext. admin-user) "fileUpload" nil)))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "fileUpload" nil)))

  (is (= true (.permit authorizer (ReqContext. user-a) "getNimbusConf" nil)))
  (is (= true (.permit authorizer (ReqContext. user-b) "getNimbusConf" nil)))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getNimbusConf" nil)))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "getNimbusConf" nil)))

  (is (= true (.permit authorizer (ReqContext. user-a) "getClusterInfo" nil)))
  (is (= true (.permit authorizer (ReqContext. user-b) "getClusterInfo" nil)))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getClusterInfo" nil)))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "getClusterInfo" nil)))

  (is (= false (.permit authorizer (ReqContext. user-a) "fileDownload" nil)))
  (is (= false (.permit authorizer (ReqContext. user-b) "fileDownload" nil)))
  (is (= true (.permit authorizer (ReqContext. admin-user) "fileDownload" nil)))
  (is (= true (.permit authorizer (ReqContext. supervisor-user) "fileDownload" nil)))

  (is (= true (.permit authorizer (ReqContext. user-a) "killTopology" {TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "killTopology" {TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "killTopology" {TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "killTopolgy" {TOPOLOGY-USERS ["user-a"]})))

  (is (= true (.permit authorizer (ReqContext. user-a) "rebalance" {TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "rebalance" {TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "rebalance" {TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "rebalance" {TOPOLOGY-USERS ["user-a"]})))

  (is (= true (.permit authorizer (ReqContext. user-a) "activate" {TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "activate" {TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "activate" {TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "activate" {TOPOLOGY-USERS ["user-a"]})))

  (is (= true (.permit authorizer (ReqContext. user-a) "deactivate" {TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "deactivate" {TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "deactivate" {TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "deactivate" {TOPOLOGY-USERS ["user-a"]})))

  (is (= true (.permit authorizer (ReqContext. user-a) "getTopologyConf" {TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "getTopologyConf" {TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getTopologyConf" {TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "getTopologyConf" {TOPOLOGY-USERS ["user-a"]})))

  (is (= true (.permit authorizer (ReqContext. user-a) "getTopology" {TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "getTopology" {TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getTopology" {TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "getTopology" {TOPOLOGY-USERS ["user-a"]})))

  (is (= true (.permit authorizer (ReqContext. user-a) "getUserTopology" {TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "getUserTopology" {TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getUserTopology" {TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "getUserTopology" {TOPOLOGY-USERS ["user-a"]})))

  (is (= true (.permit authorizer (ReqContext. user-a) "getTopologyInfo" {TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "getTopologyInfo" {TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getTopologyInfo" {TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "getTopologyInfo" {TOPOLOGY-USERS ["user-a"]})))
))

(deftest simple-acl-same-user-auth-test
  (let [cluster-conf (merge (read-storm-config)
                       {NIMBUS-ADMINS ["admin"]
                        NIMBUS-SUPERVISOR-USERS ["admin"]})
        authorizer (SimpleACLAuthorizer. )
        admin-user (mk-subject "admin")]
  (.prepare authorizer cluster-conf) 
  (is (= true (.permit authorizer (ReqContext. admin-user) "submitTopology" {})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "fileUpload" nil)))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getNimbusConf" nil)))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getClusterInfo" nil)))
  (is (= true (.permit authorizer (ReqContext. admin-user) "fileDownload" nil)))
  (is (= true (.permit authorizer (ReqContext. admin-user) "killTopology" {TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "rebalance" {TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "activate" {TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "deactivate" {TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getTopologyConf" {TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getTopology" {TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getUserTopology" {TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getTopologyInfo" {TOPOLOGY-USERS ["user-a"]})))
))


(deftest positive-authorization-test 
  (let [a-port (available-port)]
    (with-server [a-port nil
                  "backtype.storm.security.auth.authorizer.NoopAuthorizer" 
                  "backtype.storm.security.auth.SimpleTransportPlugin" nil]
      (let [storm-conf (merge (read-storm-config)
                              {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"})
            client (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)
            nimbus_client (.getClient client)]
        (testing "(Positive authorization) Authorization plugin should accept client request"
          (.activate nimbus_client "security_auth_test_topology"))
        (.close client)))))

(deftest deny-authorization-test 
  (let [a-port (available-port)]
    (with-server [a-port nil
                  "backtype.storm.security.auth.authorizer.DenyAuthorizer" 
                  "backtype.storm.security.auth.SimpleTransportPlugin" nil]
      (let [storm-conf (merge (read-storm-config)
                              {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                               Config/NIMBUS_HOST "localhost"
                               Config/NIMBUS_THRIFT_PORT a-port
                               Config/NIMBUS_TASK_TIMEOUT_SECS nimbus-timeout})
            client (NimbusClient/getConfiguredClient storm-conf)
            nimbus_client (.getClient client)]
        (testing "(Negative authorization) Authorization plugin should reject client request"
          (is (thrown-cause? AuthorizationException
                             (.activate nimbus_client "security_auth_test_topology"))))
        (.close client)))))

(deftest digest-authentication-test
  (let [a-port (available-port)]
    (with-server [a-port
                  "test/clj/backtype/storm/security/auth/jaas_digest.conf" 
                  nil
                  "backtype.storm.security.auth.digest.DigestSaslTransportPlugin" nil]
      (let [storm-conf (merge (read-storm-config)
                              {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                               STORM-NIMBUS-RETRY-TIMES 0})
            client (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)
            nimbus_client (.getClient client)]
        (testing "(Positive authentication) valid digest authentication"
          (.activate nimbus_client "security_auth_test_topology"))
        (.close client))
      
      (let [storm-conf (merge (read-storm-config)
                              {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                               STORM-NIMBUS-RETRY-TIMES 0})
            client (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)
            nimbus_client (.getClient client)]
        (testing "(Negative authentication) Server: Digest vs. Client: Simple"
          (is (thrown-cause? java.net.SocketTimeoutException
                             (.activate nimbus_client "security_auth_test_topology"))))
        (.close client))
      
      (let [storm-conf (merge (read-storm-config)
                              {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest_bad_password.conf"
                               STORM-NIMBUS-RETRY-TIMES 0})]
        (testing "(Negative authentication) Invalid  password"
          (is (thrown-cause? TTransportException
                             (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)))))
      
      (let [storm-conf (merge (read-storm-config)
                              {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest_unknown_user.conf"
                               STORM-NIMBUS-RETRY-TIMES 0})]
        (testing "(Negative authentication) Unknown user"
          (is (thrown-cause? TTransportException 
                             (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)))))

      (let [storm-conf (merge (read-storm-config)
                              {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/nonexistent.conf"
                               STORM-NIMBUS-RETRY-TIMES 0})]
        (testing "(Negative authentication) nonexistent configuration file"
          (is (thrown-cause? RuntimeException 
                             (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)))))
      
      (let [storm-conf (merge (read-storm-config)
                              {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest_missing_client.conf"
                               STORM-NIMBUS-RETRY-TIMES 0})]
        (testing "(Negative authentication) Missing client"
          (is (thrown-cause? java.io.IOException
                             (NimbusClient. storm-conf "localhost" a-port nimbus-timeout))))))))
        
(deftest test-GetTransportPlugin-throws-RuntimeException
  (let [conf (merge (read-storm-config)
                    {Config/STORM_THRIFT_TRANSPORT_PLUGIN "null.invalid"})]
    (is (thrown-cause? RuntimeException (AuthUtils/GetTransportPlugin conf nil nil)))))
