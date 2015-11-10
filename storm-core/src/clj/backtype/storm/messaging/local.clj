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
(ns backtype.storm.messaging.local
  (:refer-clojure :exclude [send])
  (:use [backtype.storm log util])
  (:import [backtype.storm.messaging IContext IConnection TaskMessage])
  (:import [backtype.storm.grouping Load])
  (:import [java.util.concurrent LinkedBlockingQueue])
  (:import [java.util Map Iterator Collection])
  (:import [java.util Iterator ArrayList])
  (:gen-class))

(defn update-load! [cached-task->load lock task->load]
  (locking lock
    (swap! cached-task->load merge task->load)))

(defn add-queue! [queues-map lock storm-id port]
  (let [id (str storm-id "-" port)]
    (locking lock
      (when-not (contains? @queues-map id)
        (swap! queues-map assoc id (LinkedBlockingQueue.))))
    (@queues-map id)))

(deftype LocalConnection [storm-id port queues-map lock queue task->load]
  IConnection
  (^Iterator recv [this ^int flags ^int clientId]
    (when-not queue
      (throw (IllegalArgumentException. "Cannot receive on this socket")))
    (let [ret (ArrayList.)
          msg (if (= flags 1) (.poll queue) (.take queue))]
      (if msg
        (do 
          (.add ret msg)
          (.iterator ret))
        nil)))
  (^void send [this ^int taskId ^bytes payload]
    (let [send-queue (add-queue! queues-map lock storm-id port)]
      (.put send-queue (TaskMessage. taskId payload))
      ))
  (^void send [this ^Iterator iter]
    (let [send-queue (add-queue! queues-map lock storm-id port)]
      (while (.hasNext iter) 
         (.put send-queue (.next iter)))
      ))
  (^void sendLoadMetrics [this ^Map taskToLoad]
    (update-load! task->load lock taskToLoad))
  (^Map getLoad [this ^Collection tasks]
    (locking lock
      (into {}
        (for [task tasks
              :let [load (.get @task->load task)]
              :when (not-nil? load)]
          ;; for now we are ignoring the connection load locally
          [task (Load. true load 0.0)]))))
  (^void close [this]))


(deftype LocalContext [^{:unsynchronized-mutable true} queues-map
                       ^{:unsynchronized-mutable true} lock
                       ^{:unsynchronized-mutable true} task->load]
  IContext
  (^void prepare [this ^Map storm-conf]
    (set! queues-map (atom {}))
    (set! task->load (atom {}))
    (set! lock (Object.)))
  (^IConnection bind [this ^String storm-id ^int port]
    (LocalConnection. storm-id port queues-map lock (add-queue! queues-map lock storm-id port) task->load))
  (^IConnection connect [this ^String storm-id ^String host ^int port]
    (LocalConnection. storm-id port queues-map lock nil task->load))
  (^void term [this]
    ))

(defn mk-context [] 
  (let [context  (LocalContext. nil nil nil)]
    (.prepare ^IContext context nil)
    context))
