(ns backtype.storm.security.auth.digest-auth-test
  (:use [clojure test])
  (:require [backtype.storm.daemon [nimbus :as nimbus]])
  (:import [org.apache.thrift7 TException])
  (:import [java.nio.channels Channels WritableByteChannel])
  (:import [backtype.storm.utils NimbusClient])
  (:import [backtype.storm.security.auth ThriftServer ThriftClient ReqContext ReqContext$OperationType])
  (:use [backtype.storm bootstrap util])
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap testing])
  (:import [backtype.storm.generated Nimbus Nimbus$Client])
  )

(bootstrap)

(def server-port 6627)
 
; Exceptions are getting wrapped in RuntimeException.  This might be due to
; CLJ-855.
(defn- unpack-runtime-exception [expression]
  (try (eval expression)
    nil
    (catch java.lang.RuntimeException gripe
      (throw (.getCause gripe)))
  )
)

(defn launch-test-server []
  (with-inprocess-zookeeper zk-port
    (with-local-tmp [nimbus-dir]
      (let [conf (merge (read-storm-config)
                     {STORM-ZOOKEEPER-SERVERS ["localhost"]
                      STORM-ZOOKEEPER-PORT zk-port
		      NIMBUS-HOST "localhost"
		      NIMBUS-THRIFT-PORT server-port
                      STORM-LOCAL-DIR nimbus-dir})
           nimbus (nimbus/standalone-nimbus)
           service-handler (nimbus/service-handler conf nimbus)
           server (ThriftServer. (Nimbus$Processor. service-handler) (int (conf NIMBUS-THRIFT-PORT)))]
    	   (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.shutdown service-handler) (.stop server))))
    	   (log-message "Starting Nimbus server...")
    	   (.serve server)))))

(defn launch-server-w-wait []
   (future (launch-test-server))
   (log-message "Waiting for Nimbus Server...")
   (Thread/sleep 10000))

(deftest digest-auth-test 
  (System/setProperty "java.security.auth.login.config" "./conf/jaas_digest.conf")
  (launch-server-w-wait)
  (log-message "Starting Nimbus client w/ connection to localhost:" server-port)
  (let [client (NimbusClient. "localhost" server-port)
        nimbus_client (.getClient client)]
     (is (thrown? backtype.storm.generated.NotAliveException (.activate nimbus_client "bogus_topology")))
     (.close client)))
