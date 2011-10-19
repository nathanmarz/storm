(ns backtype.storm.daemon.drpc
  (:import [org.apache.thrift.server THsHaServer THsHaServer$Args])
  (:import [org.apache.thrift.protocol TBinaryProtocol TBinaryProtocol$Factory])
  (:import [org.apache.thrift TException])
  (:import [org.apache.thrift.transport TNonblockingServerTransport TNonblockingServerSocket])
  (:import [backtype.storm.generated DistributedRPC DistributedRPC$Iface DistributedRPC$Processor])
  (:import [java.util.concurrent Semaphore])
  (:import [backtype.storm.drpc SpoutAdder])
  (:import [java.net InetAddress])
  (:use [backtype.storm bootstrap])
  (:gen-class))

(bootstrap)


(def DEFAULT-PORT 3772)  ; "drpc"
(def REQUEST-TIMEOUT-SECS 600)
(def TIMEOUT-CHECK-SECS 60)

;; TODO: change this to use TimeCacheMap
(defn service-handler [^SpoutAdder spout-adder port]
  (let [ctr (atom 0)
        id->sem (atom {})
        id->result (atom {})
        id->start (atom {})
        cleanup (fn [id] (swap! id->sem dissoc id)
                         (swap! id->result dissoc id)
                         (swap! id->start dissoc id))
        my-ip (.getHostAddress (InetAddress/getLocalHost))
        ]
    (async-loop
      (fn []
        (doseq [[id start] @id->start]
          (when (> (time-delta start) REQUEST-TIMEOUT-SECS)
            (if-let [sem (@id->sem id)]
              (.release sem))
            (cleanup id)
            ))
        TIMEOUT-CHECK-SECS
        ))
    (reify DistributedRPC$Iface
      (^String execute [this ^String function ^String args]
        (let [id (str (swap! ctr (fn [v] (mod (inc v) 1000000000))))
              ^Semaphore sem (Semaphore. 0)
              return-info (to-json {"ip" my-ip "port" port "id" id})
              ]
          (swap! id->start assoc id (current-time-secs))
          (swap! id->sem assoc id sem)
          (.add spout-adder function args return-info)
          (.acquire sem)
          (let [result (@id->result id)]
            (cleanup id)
            result
            )))
      (^void result [this ^String id ^String result]
        (let [^Semaphore sem (@id->sem id)]
          (when sem
            (swap! id->result assoc id result)
            (.release sem)
            )))
      )))

(defn launch-server!
  ([spout-adder]
    (launch-server! DEFAULT-PORT spout-adder))
  ([port spout-adder]
    (let [service-handler (service-handler spout-adder port)          
          options (THsHaServer$Args. (TNonblockingServerSocket. port))
          _ (set! (. options maxWorkerThreads) 64)
          _ (set! (. options processor) (DistributedRPC$Processor. service-handler)) 
          _ (set! (. options protocolFactory) (TBinaryProtocol$Factory.))
          server (THsHaServer. options)]
      (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.stop server))))
      (log-message "Starting Distributed RPC server...")
      (.serve server))))

(defn -main [spout-adder-class & args]
  (let [form (concat ['new (symbol spout-adder-class)] args)]
    (launch-server! (eval form))
    ))
