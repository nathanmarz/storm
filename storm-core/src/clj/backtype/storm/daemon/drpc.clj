;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns backtype.storm.daemon.drpc
  (:import [org.apache.thrift.server THsHaServer THsHaServer$Args])
  (:import [org.apache.thrift.protocol TBinaryProtocol TBinaryProtocol$Factory])
  (:import [org.apache.thrift.exception])
  (:import [org.apache.thrift.transport
            TNonblockingServerTransport TNonblockingServerSocket])
  (:import [backtype.storm.generated
            DistributedRPC DistributedRPC$Iface DistributedRPC$Processor
            DRPCRequest DRPCExecutionException
            DistributedRPCInvocations DistributedRPCInvocations$Iface
            DistributedRPCInvocations$Processor])
  (:import [java.util.concurrent Semaphore ConcurrentLinkedQueue
            ThreadPoolExecutor ArrayBlockingQueue TimeUnit])
  (:import [backtype.storm.daemon Shutdownable])
  (:import [java.net InetAddress])
  (:use [backtype.storm bootstrap config log])
  (:gen-class))

(bootstrap)

(defn timeout-check-secs [] 5)

(defn acquire-queue [queues-atom function]
  (swap! queues-atom
    (fn [amap]
      (if-not (amap function)
        (assoc amap function (ConcurrentLinkedQueue.))
        amap)))
  (@queues-atom function))

;; TODO: change this to use TimeCacheMap
(defn service-handler []
  (let [conf (read-storm-config)
        ctr (atom 0)
        id->sem (atom {})
        id->result (atom {})
        id->start (atom {})
        id->function (atom {})
        id->request (atom {})
        request-queues (atom {})
        cleanup (fn [id] (swap! id->sem dissoc id)
                  (swap! id->result dissoc id)
                  (swap! id->function dissoc id)
                  (swap! id->request dissoc id)
                  (swap! id->start dissoc id))
        my-ip (.getHostAddress (InetAddress/getLocalHost))
        clear-thread (async-loop
                       (fn []
                         (doseq [[id start] @id->start]
                           (when (> (time-delta start) (conf DRPC-REQUEST-TIMEOUT-SECS))
                             (when-let [sem (@id->sem id)]
                               (.remove (acquire-queue request-queues (@id->function id)) (@id->request id))
                               (log-warn "Timeout DRPC request id: " id " start at " start)
                               (.release sem))
                             (cleanup id)))
                         (timeout-check-secs)))]
    (reify DistributedRPC$Iface

      (^String execute
        [this ^String function ^String args]
        (log-debug "Received DRPC request for " function " " args " at " (System/currentTimeMillis))
        (let [id (str (swap! ctr (fn [v] (mod (inc v) 1000000000))))
              ^Semaphore sem (Semaphore. 0)
              req (DRPCRequest. args id)
              ^ConcurrentLinkedQueue queue (acquire-queue request-queues function)]
          (swap! id->start assoc id (current-time-secs))
          (swap! id->sem assoc id sem)
          (swap! id->function assoc id function)
          (swap! id->request assoc id req)
          (.add queue req)
          (log-debug "Waiting for DRPC result for " function " " args " at " (System/currentTimeMillis))
          (.acquire sem)
          (log-debug "Acquired DRPC result for " function " " args " at " (System/currentTimeMillis))
          (let [result (@id->result id)]
            (cleanup id)
            (log-debug "Returning DRPC result for " function " " args " at " (System/currentTimeMillis))
            (if (instance? DRPCExecutionException result)
              (throw result)
              (if (nil? result)
                (throw (DRPCExecutionException. "Request timed out"))
                result)))))

      DistributedRPCInvocations$Iface

      (^void result
        [this ^String id ^String result]
        (let [^Semaphore sem (@id->sem id)]
          (log-debug "Received result " result " for " id " at " (System/currentTimeMillis))
          (when sem
            (swap! id->result assoc id result)
            (.release sem))))

      (^void failRequest
        [this ^String id]
        (let [^Semaphore sem (@id->sem id)]
          (when sem
            (swap! id->result assoc id (DRPCExecutionException. "Request failed"))
            (.release sem))))

      (^DRPCRequest fetchRequest
        [this ^String func]
        (let [^ConcurrentLinkedQueue queue (acquire-queue request-queues func)
              ret (.poll queue)]
          (if ret
            (do (log-debug "Fetched request for " func " at " (System/currentTimeMillis))
              ret)
            (DRPCRequest. "" ""))))

      Shutdownable

      (shutdown
        [this]
        (.interrupt clear-thread)))))

(defn launch-server!
  ([]
    (let [conf (read-storm-config)
          worker-threads (int (conf DRPC-WORKER-THREADS))
          queue-size (int (conf DRPC-QUEUE-SIZE))
          service-handler (service-handler)

          ;; Requests and returns need to be on separate thread pools, since
          ;; calls to "execute" don't unblock until other thrift methods are
          ;; called. So if 64 threads are calling execute, the server won't
          ;; accept the result invocations that will unblock those threads.

          handler-server
          (THsHaServer. (-> (TNonblockingServerSocket. (int (conf DRPC-PORT)))
                            (THsHaServer$Args.)
                            (.workerThreads 64)
                            (.executorService
                              (ThreadPoolExecutor.
                                worker-threads worker-threads 60 TimeUnit/SECONDS
                                (ArrayBlockingQueue. queue-size)))
                            (.protocolFactory (TBinaryProtocol$Factory.))
                            (.processor (DistributedRPC$Processor. service-handler))))

          invoke-server
          (THsHaServer. (-> (TNonblockingServerSocket. (int (conf DRPC-INVOCATIONS-PORT)))
                            (THsHaServer$Args.)
                            (.workerThreads 64)
                            (.protocolFactory (TBinaryProtocol$Factory.))
                            (.processor
                              (DistributedRPCInvocations$Processor. service-handler))))]

      (.addShutdownHook
        (Runtime/getRuntime)
        (Thread. (fn [] (.stop handler-server) (.stop invoke-server))))
      (log-message "Starting Distributed RPC servers...")
      (future (.serve invoke-server))
      (.serve handler-server))))

(defn -main []
  (launch-server!))
