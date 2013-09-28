(ns backtype.storm.security.auth.drpc-auth-test
  (:use [clojure test])
  (:require [backtype.storm.daemon [drpc :as drpc]])
  (:import [backtype.storm.generated AuthorizationException])
  (:import [backtype.storm Config])
  (:import [backtype.storm.security.auth ThriftServer]) 
  (:import [backtype.storm.utils DRPCClient])
  (:import [backtype.storm.drpc DRPCInvocationsClient])
  (:import [backtype.storm.generated DistributedRPC$Processor DistributedRPCInvocations$Processor])
  (:use [backtype.storm bootstrap util])
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap testing])
  )

(bootstrap)

(def drpc-timeout (Integer. 30))

(defn launch-server [conf drpcAznClass invocationsAznClass transportPluginClass login-cfg] 
  (let [conf (if drpcAznClass (assoc conf DRPC-AUTHORIZER drpcAznClass) conf)
        conf (if invocationsAznClass (assoc conf DRPC-INVOCATIONS-AUTHORIZER invocationsAznClass) conf)
        conf (if transportPluginClass (assoc conf STORM-THRIFT-TRANSPORT-PLUGIN transportPluginClass) conf)
        conf (if login-cfg (assoc conf "java.security.auth.login.config" login-cfg) conf)
        service-handler (drpc/service-handler conf)
        handler-server (ThriftServer. conf 
                                      (DistributedRPC$Processor. service-handler) 
                                      (int (conf DRPC-PORT)) backtype.storm.Config$ThriftServerPurpose/DRPC)
        invoke-server (ThriftServer. conf 
                                     (DistributedRPCInvocations$Processor. service-handler) 
                                     (int (conf DRPC-INVOCATIONS-PORT)) backtype.storm.Config$ThriftServerPurpose/DRPC)]      
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
  (let [storm-conf (read-storm-config)]
    (with-server [storm-conf "backtype.storm.security.auth.authorizer.DenyAuthorizer"  
                  "backtype.storm.security.auth.authorizer.DenyAuthorizer"  
                  nil nil]
      (let [drpc (DRPCClient. storm-conf "localhost" (storm-conf DRPC-PORT))
            drpc_client (.getClient drpc)
            invocations (DRPCInvocationsClient. storm-conf "localhost" (storm-conf DRPC-INVOCATIONS-PORT))
            invocations_client (.getClient invocations)]
        (is (thrown? AuthorizationException (.execute drpc_client "func-foo" "args-bar")))
        (is (thrown? AuthorizationException (.result invocations_client nil nil)))
        (is (thrown? AuthorizationException (.fetchRequest invocations_client nil)))
        (is (thrown? AuthorizationException (.failRequest invocations_client nil)))
        (.close drpc)
        (.close invocations)))))

(deftest deny-drpc-digest-test 
  (let [storm-conf (read-storm-config)]
    (with-server [storm-conf "backtype.storm.security.auth.authorizer.DenyAuthorizer"  
                  "backtype.storm.security.auth.authorizer.DenyAuthorizer"  
                  "backtype.storm.security.auth.digest.DigestSaslTransportPlugin" 
                  "test/clj/backtype/storm/security/auth/jaas_digest.conf"]
      (let [conf (merge storm-conf {STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"})
            drpc (DRPCClient. conf "localhost" (storm-conf DRPC-PORT))
            drpc_client (.getClient drpc)
            invocations (DRPCInvocationsClient. conf "localhost" (storm-conf DRPC-INVOCATIONS-PORT))
            invocations_client (.getClient invocations)]
        (is (thrown? AuthorizationException (.execute drpc_client "func-foo" "args-bar")))
        (is (thrown? AuthorizationException (.result invocations_client nil nil)))
        (is (thrown? AuthorizationException (.fetchRequest invocations_client nil)))
        (is (thrown? AuthorizationException (.failRequest invocations_client nil)))
        (.close drpc)
        (.close invocations)))))
