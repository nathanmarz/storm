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
(ns backtype.storm.messaging.loader
  (:use [backtype.storm util log])
  (:import [java.util ArrayList Iterator])
  (:import [backtype.storm.messaging IContext IConnection TaskMessage])
  (:import [backtype.storm.utils DisruptorQueue MutableObject])
  (:require [backtype.storm.messaging [local :as local]])
  (:require [backtype.storm [disruptor :as disruptor]]))

(defn mk-local-context []
  (local/mk-context))

(defn- mk-receive-thread [context storm-id port transfer-local-fn  daemon kill-fn priority socket max-buffer-size thread-id]
    (async-loop
       (fn []
         (log-message "Starting receive-thread: [stormId: " storm-id ", port: " port ", thread-id: " thread-id  " ]")
         (fn []
           (let [batched (ArrayList.)
                 ^Iterator iter (.recv ^IConnection socket 0 thread-id)
                 closed (atom false)]
             (when iter
               (while (and (not @closed) (.hasNext iter)) 
                  (let [packet (.next iter)
                        task (if packet (.task ^TaskMessage packet))
                        message (if packet (.message ^TaskMessage packet))]
                      (if (= task -1)
                         (do (log-message "Receiving-thread:[" storm-id ", " port "] received shutdown notice")
                           (.close socket)
                           (reset! closed  true))
                         (when packet (.add batched [task message]))))))
             
             (when (not @closed)
               (do
                 (if (> (.size batched) 0)
                   (transfer-local-fn batched))
                 0)))))
         :factory? true
         :daemon daemon
         :kill-fn kill-fn
         :priority priority
         :thread-name (str "worker-receiver-thread-" thread-id)))

(defn- mk-receive-threads [context storm-id port transfer-local-fn  daemon kill-fn priority socket max-buffer-size thread-count]
  (into [] (for [thread-id (range thread-count)] 
             (mk-receive-thread context storm-id port transfer-local-fn  daemon kill-fn priority socket max-buffer-size thread-id))))


(defnk launch-receive-thread!
  [context storm-id receiver-thread-count port transfer-local-fn max-buffer-size
   :daemon true
   :kill-fn (fn [t] (System/exit 1))
   :priority Thread/NORM_PRIORITY]
  (let [max-buffer-size (int max-buffer-size)
        socket (.bind ^IContext context storm-id port)
        thread-count (if receiver-thread-count receiver-thread-count 1)
        vthreads (mk-receive-threads context storm-id port transfer-local-fn daemon kill-fn priority socket max-buffer-size thread-count)]
    (fn []
      (let [kill-socket (.connect ^IContext context storm-id "localhost" port)]
        (log-message "Shutting down receiving-thread: [" storm-id ", " port "]")
        (.send ^IConnection kill-socket
                  -1 (byte-array []))
        
        (.close ^IConnection kill-socket)
        
        (log-message "Waiting for receiving-thread:[" storm-id ", " port "] to die")
        
        (for [thread-id (range thread-count)] 
             (.join (vthreads thread-id)))
        
        (log-message "Shutdown receiving-thread: [" storm-id ", " port "]")
        ))))