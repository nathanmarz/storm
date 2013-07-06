(ns backtype.storm.dynamic-port-test
  (:use [clojure test])
  (:require [backtype.storm.daemon [nimbus :as nimbus]])  
  (:require [backtype.storm.ui [core :as ui]])  
  (:import [backtype.storm.testing TestWordCounter TestWordSpout TestGlobalCount TestAggregatesCounter])
  (:import [backtype.storm.security.auth ThriftServer])
  (:import [backtype.storm.scheduler INimbus])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  )

(bootstrap)

(defn launch-nimbus-server [conf ^Nimbus$Iface service-handler] 
  (let [port (Integer. (conf NIMBUS-THRIFT-PORT))
        server (ThriftServer. conf (Nimbus$Processor. service-handler) port)]
    (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.stop server))))
    (.start (Thread. #(.serve server)))
    (wait-for-condition #(.isServing server))
    server ))

(defmacro with-server [[cluster-sym & args] & body]
  `(let [~cluster-sym (mk-local-storm-cluster ~@args)
         conf#   (:daemon-conf ~cluster-sym) 
         service-handler# (:nimbus ~cluster-sym)
         server# (launch-nimbus-server conf# service-handler#)]
     (try
       ~@body
       (catch Throwable t#
         (log-error t# "Error in cluster")
         (throw t#)
         )
       (finally
         (try 
           (kill-local-storm-cluster ~cluster-sym)
           (.stop server#)
           (catch Throwable t1#))))
     ))

(deftest test-dynamic-nimbus-port
  (with-server [cluster 
                :supervisors 4 
                :daemon-conf {STORM-LOCAL-MODE-ZMQ true 
                              NIMBUS-THRIFT-PORT 0}]
    (let [conf (:daemon-conf cluster)
          topology (thrift/mk-topology
                     {"1" (thrift/mk-spout-spec (TestWordSpout. true) :parallelism-hint 4)}
                     {"2" (thrift/mk-bolt-spec {"1" :shuffle} (TestGlobalCount.)
                                               :parallelism-hint 6)})
          results (complete-topology cluster
                                     topology
                                     ;; important for test that
                                     ;; #tuples = multiple of 4 and 6
                                     :storm-conf {TOPOLOGY-WORKERS 3}
                                     :mock-sources {"1" [["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ]})]
      (is (pos? (Integer. (conf NIMBUS-THRIFT-PORT))))
      (is (ms= (apply concat (repeat 6 [[1] [2] [3] [4]]))
               (read-tuples results "2"))))))

(deftest test-ui-access-nimbus
  (with-server [cluster 
                :supervisors 0
                :daemon-conf {STORM-LOCAL-MODE-ZMQ true 
                              NIMBUS-THRIFT-PORT -1}]
    (let [conf (:daemon-conf cluster)
          ui-server-app (ui/app conf)
          req {:uri "/" :request-method :get}
          resp (ui-server-app req)]
      (is (pos? (Integer. (conf NIMBUS-THRIFT-PORT))))
      (log-message "ui server app:" ui-server-app)
      (is (= 200 (:status resp)))
      (is (pos? (.indexOf (:body resp) "Cluster Summary"))))))
