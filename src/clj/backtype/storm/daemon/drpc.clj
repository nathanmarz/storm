(ns backtype.storm.daemon.drpc
  (:import [org.apache.thrift7.server THsHaServer THsHaServer$Args])
  (:import [org.apache.thrift7.protocol TBinaryProtocol TBinaryProtocol$Factory])
  (:import [org.apache.thrift7 TException])
  (:import [org.apache.thrift7.transport TNonblockingServerTransport TNonblockingServerSocket])
  (:import [backtype.storm.generated DistributedRPC DistributedRPC$Iface DistributedRPC$Processor DRPCRequest DRPCExecutionException])
  (:import [java.util.concurrent Semaphore ConcurrentLinkedQueue])
  (:import [backtype.storm.daemon Shutdownable])
  (:import [java.net InetAddress])
  (:use [backtype.storm bootstrap config log])
  (:gen-class))

(bootstrap)

;; TODO: timeout should be configurable on a per-function basis
(def REQUEST-TIMEOUT-SECS 60)
(def TIMEOUT-CHECK-SECS 5)

(defn acquire-queue [queues-atom function]
  (swap! queues-atom
    (fn [amap]
      (if-not (amap function)
        (assoc amap function (ConcurrentLinkedQueue.))
        amap)
        ))
  (@queues-atom function))

;; TODO: change this to use TimeCacheMap
(defn service-handler []
  (let [ctr (atom 0)
        id->sem (atom {})
        id->result (atom {})
        id->start (atom {})
        request-queues (atom {})
        cleanup (fn [id] (swap! id->sem dissoc id)
                         (swap! id->result dissoc id)
                         (swap! id->start dissoc id))
        my-ip (.getHostAddress (InetAddress/getLocalHost))
        clear-thread (async-loop
                      (fn []
                        (doseq [[id start] @id->start]
                          (when (> (time-delta start) REQUEST-TIMEOUT-SECS)
                            (when-let [sem (@id->sem id)]
                              (swap! id->result assoc id (DRPCExecutionException. "Request timed out"))
                              (.release sem))
                            (cleanup id)
                            ))
                        TIMEOUT-CHECK-SECS
                        ))
        ]
    (reify DistributedRPC$Iface
      (^String execute [this ^String function ^String args]
        (log-debug "Received DRPC request for " function " " args " at " (System/currentTimeMillis))
        (let [id (str (swap! ctr (fn [v] (mod (inc v) 1000000000))))
              ^Semaphore sem (Semaphore. 0)
              req (DRPCRequest. args id)
              ^ConcurrentLinkedQueue queue (acquire-queue request-queues function)
              ]
          (swap! id->start assoc id (current-time-secs))
          (swap! id->sem assoc id sem)
          (.add queue req)
          (log-debug "Waiting for DRPC result for " function " " args " at " (System/currentTimeMillis))
          (.acquire sem)
          (log-debug "Acquired DRPC result for " function " " args " at " (System/currentTimeMillis))
          (let [result (@id->result id)]
            (cleanup id)
            (log-debug "Returning DRPC result for " function " " args " at " (System/currentTimeMillis))
            (if (instance? DRPCExecutionException result)
              (throw result)
              result
              ))))
      (^void result [this ^String id ^String result]
        (let [^Semaphore sem (@id->sem id)]
          (log-debug "Received result " result " for " id " at " (System/currentTimeMillis))
          (when sem
            (swap! id->result assoc id result)
            (.release sem)
            )))
      (^void failRequest [this ^String id]
        (let [^Semaphore sem (@id->sem id)]
          (when sem
            (swap! id->result assoc id (DRPCExecutionException. "Request failed"))
            (.release sem)
            )))
      (^DRPCRequest fetchRequest [this ^String func]
        (let [^ConcurrentLinkedQueue queue (acquire-queue request-queues func)
              ret (.poll queue)]
          (if ret
            (do (log-debug "Fetched request for " func " at " (System/currentTimeMillis))
                ret)
            (DRPCRequest. "" ""))
          ))
      Shutdownable
      (shutdown [this]
        (.interrupt clear-thread))
      )))

(defn launch-server!
  ([]
    (let [conf (read-storm-config)
          service-handler (service-handler)     
          options (-> (TNonblockingServerSocket. (int (conf DRPC-PORT)))
                    (THsHaServer$Args.)
                    (.workerThreads 64)
                    (.protocolFactory (TBinaryProtocol$Factory.))
                    (.processor (DistributedRPC$Processor. service-handler))
                    )
          server (THsHaServer. options)]
      (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.stop server))))
      (log-message "Starting Distributed RPC server...")
      (.serve server))))

(defn -main []
  (launch-server!))
