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

(ns org.apache.storm.disruptor
  (:import [org.apache.storm.utils DisruptorQueue WorkerBackpressureCallback DisruptorBackpressureCallback])
  (:import [com.lmax.disruptor.dsl ProducerType])
  (:require [clojure [string :as str]])
  (:require [clojure [set :as set]])
  (:use [clojure walk])
  (:use [org.apache.storm util log]))

(def PRODUCER-TYPE
  {:multi-threaded ProducerType/MULTI
   :single-threaded ProducerType/SINGLE})

(defnk disruptor-queue
  [^String queue-name buffer-size timeout :producer-type :multi-threaded :batch-size 100 :batch-timeout 1]
  (DisruptorQueue. queue-name
                   (PRODUCER-TYPE producer-type) buffer-size
                   timeout batch-size batch-timeout))

(defn clojure-handler
  [afn]
  (reify com.lmax.disruptor.EventHandler
    (onEvent
      [this o seq-id batchEnd?]
      (afn o seq-id batchEnd?))))

(defn disruptor-backpressure-handler
  [afn-high-wm afn-low-wm]
  (reify DisruptorBackpressureCallback
    (highWaterMark
      [this]
      (afn-high-wm))
    (lowWaterMark
      [this]
      (afn-low-wm))))

(defn worker-backpressure-handler
  [afn]
  (reify WorkerBackpressureCallback
    (onEvent
      [this o]
      (afn o))))

(defmacro handler
  [& args]
  `(clojure-handler (fn ~@args)))

(defn publish
  [^DisruptorQueue q o]
  (.publish q o))

(defn consume-batch
  [^DisruptorQueue queue handler]
  (.consumeBatch queue handler))

(defn consume-batch-when-available
  [^DisruptorQueue queue handler]
  (.consumeBatchWhenAvailable queue handler))

(defn halt-with-interrupt!
  [^DisruptorQueue queue]
  (.haltWithInterrupt queue))

(defnk consume-loop*
  [^DisruptorQueue queue handler
   :kill-fn (fn [error] (exit-process! 1 "Async loop died!"))]
  (async-loop
          (fn [] (consume-batch-when-available queue handler) 0)
          :kill-fn kill-fn
          :thread-name (.getName queue)))

(defmacro consume-loop [queue & handler-args]
  `(let [handler# (handler ~@handler-args)]
     (consume-loop* ~queue handler#)))
