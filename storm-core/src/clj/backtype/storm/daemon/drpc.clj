(ns backtype.storm.daemon.drpc
  (:import [org.apache.thrift7.server THsHaServer THsHaServer$Args])
  (:import [org.apache.thrift7.protocol TBinaryProtocol TBinaryProtocol$Factory])
  (:import [org.apache.thrift7 TException])
  (:import [org.apache.thrift7.transport TNonblockingServerTransport TNonblockingServerSocket])
  (:import [backtype.storm.generated DistributedRPC DistributedRPC$Iface DistributedRPC$Processor
            DRPCRequest DRPCExecutionException DistributedRPCInvocations DistributedRPCInvocations$Iface
            DistributedRPCInvocations$Processor])
  (:import [java.util.concurrent Semaphore ConcurrentLinkedQueue ThreadPoolExecutor ArrayBlockingQueue TimeUnit])
  (:import [backtype.storm.daemon Shutdownable])
  (:import [java.net InetAddress])
  (:use [backtype.storm bootstrap config log])
  (:use compojure.core)
  (:use ring.middleware.reload)
  (:use [ring.adapter.jetty :only [run-jetty]])
  (:require [ring.util.response :as resp])
  (:gen-class))

(bootstrap)

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
  (let [conf (read-storm-config)
        ctr (atom 0)
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
                          (when (> (time-delta start) (conf DRPC-REQUEST-TIMEOUT-SECS))
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
      DistributedRPCInvocations$Iface
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

(defn handle-request [handler]
  (fn [request]
    (handler request)))

(defn webapp [handler]
  (->(def http-routes
      (routes
        (GET "/drpc/:func/:args" [:as {cookies :cookies} func args & m]
          (.execute handler func args))
        (GET "/drpc/:func/" [:as {cookies :cookies} func & m]
          (.execute handler func ""))
        (GET "/drpc/:func" [:as {cookies :cookies} func & m]
          (.execute handler func ""))))
    (wrap-reload '[backtype.storm.daemon.drpc])
    handle-request))

(defn launch-server!
  ([]
    (let [conf (read-storm-config)
          worker-threads (int (conf DRPC-WORKER-THREADS))
          queue-size (int (conf DRPC-QUEUE-SIZE))
          drpc-http-port (if (conf DRPC-HTTP-PORT) (int (conf DRPC-HTTP-PORT)) 0)
          drpc-port (int (conf DRPC-PORT))
          drpc-service-handler (service-handler)
          ;; requests and returns need to be on separate thread pools, since calls to
          ;; "execute" don't unblock until other thrift methods are called. So if 
          ;; 64 threads are calling execute, the server won't accept the result
          ;; invocations that will unblock those threads
          handler-server (if (> drpc-port 0)
                           (THsHaServer. (-> (TNonblockingServerSocket. drpc-port)
                                             (THsHaServer$Args.)
                                             (.workerThreads 64)
                                             (.executorService (ThreadPoolExecutor. worker-threads worker-threads 
                                                                 60 TimeUnit/SECONDS (ArrayBlockingQueue. queue-size)))
                                             (.protocolFactory (TBinaryProtocol$Factory.))
                                             (.processor (DistributedRPC$Processor. drpc-service-handler))
                                             ))
                           )
          invoke-server (THsHaServer. (-> (TNonblockingServerSocket. (int (conf DRPC-INVOCATIONS-PORT)))
                                             (THsHaServer$Args.)
                                             (.workerThreads 64)
                                             (.protocolFactory (TBinaryProtocol$Factory.))
                                             (.processor (DistributedRPCInvocations$Processor. drpc-service-handler))
                                             ))]
      (.addShutdownHook (Runtime/getRuntime) (Thread. (fn []
                                                        (if handler-server (.stop handler-server))
                                                        (.stop invoke-server))))
      (log-message "Starting Distributed RPC servers...")
      (future (.serve invoke-server))
      (if (> drpc-http-port 0)
        (run-jetty (webapp drpc-service-handler)
            {:port drpc-http-port :join? false})
        )
      (if handler-server
        (.serve handler-server)
        )
      )))

(defn -main []
  (launch-server!))
