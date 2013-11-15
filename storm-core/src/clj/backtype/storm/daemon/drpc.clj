(ns backtype.storm.daemon.drpc
  (:import [backtype.storm.security.auth ThriftServer ReqContext])
  (:import [backtype.storm.security.auth.authorizer Audit])
  (:import [org.apache.thrift7 TException])
  (:import [backtype.storm.generated DistributedRPC DistributedRPC$Iface DistributedRPC$Processor
            DRPCRequest DRPCExecutionException DistributedRPCInvocations DistributedRPCInvocations$Iface
            DistributedRPCInvocations$Processor])
  (:import [java.util.concurrent Semaphore ConcurrentLinkedQueue ThreadPoolExecutor ArrayBlockingQueue TimeUnit])
  (:import [backtype.storm.daemon Shutdownable])
  (:import [backtype.storm.security.auth IAuthorizer])
  (:import [java.net InetAddress])
  (:import [backtype.storm.generated AuthorizationException])
  (:use [backtype.storm bootstrap config log])
  (:use [backtype.storm.daemon common])
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

(defn check-authorization! [aclHandler storm-conf operation]
  (let [ctxt (ReqContext/context)]
    (log-debug "DRPC check-authorization with handler: " aclHandler)
    (Audit/log ctxt operation storm-conf)
    (if aclHandler
      (if-not (.permit aclHandler ctxt operation storm-conf)
        (throw (AuthorizationException. (str "DRPC request " operation " is not authorized")))
        ))))

;; TODO: change this to use TimeCacheMap
(defn service-handler [conf]
  (let [drpc-acl-handler (mk-authorization-handler (conf DRPC-AUTHORIZER) conf)
        invocations-acl-handler (mk-authorization-handler (conf DRPC-INVOCATIONS-AUTHORIZER) conf)
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
        (check-authorization! drpc-acl-handler conf "execute")
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
        (check-authorization! invocations-acl-handler conf "result")
        (let [^Semaphore sem (@id->sem id)]
          (log-debug "Received result " result " for " id " at " (System/currentTimeMillis))
          (when sem
            (swap! id->result assoc id result)
            (.release sem)
            )))
      (^void failRequest [this ^String id]
        (check-authorization! invocations-acl-handler conf "failRequest")
        (let [^Semaphore sem (@id->sem id)]
          (when sem
            (swap! id->result assoc id (DRPCExecutionException. "Request failed"))
            (.release sem)
            )))
      (^DRPCRequest fetchRequest [this ^String func]
        (check-authorization! invocations-acl-handler conf "fetchRequest")
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
          worker-threads (int (conf DRPC-WORKER-THREADS))
          queue-size (int (conf DRPC-QUEUE-SIZE))
          service-handler (service-handler conf)
          ;; requests and returns need to be on separate thread pools, since calls to
          ;; "execute" don't unblock until other thrift methods are called. So if 
          ;; 64 threads are calling execute, the server won't accept the result
          ;; invocations that will unblock those threads
          handler-server (ThriftServer. conf 
                                        (DistributedRPC$Processor. service-handler) 
                                        (int (conf DRPC-PORT))
                                        backtype.storm.Config$ThriftServerPurpose/DRPC
                                        (ThreadPoolExecutor. worker-threads worker-threads
                                                             60 TimeUnit/SECONDS (ArrayBlockingQueue. queue-size)))
          invoke-server (ThriftServer. conf 
                                       (DistributedRPCInvocations$Processor. service-handler) 
                                       (int (conf DRPC-INVOCATIONS-PORT))
                                       backtype.storm.Config$ThriftServerPurpose/DRPC)]
      (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.stop handler-server) (.stop invoke-server))))
      (log-message "Starting Distributed RPC servers...")
      (future (.serve invoke-server))
      (.serve handler-server))))

(defn -main []
  (launch-server!))
