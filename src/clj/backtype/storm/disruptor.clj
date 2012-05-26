(ns backtype.storm.disruptor
  (:import [backtype.storm.utils DisruptorQueue NonBlockingPairDisruptorQueue])
  (:import [com.lmax.disruptor MultiThreadedClaimStrategy SingleThreadedClaimStrategy
              BlockingWaitStrategy SleepingWaitStrategy YieldingWaitStrategy
              BusySpinWaitStrategy])
  (:require [clojure [string :as str]])
  (:require [clojure [set :as set]])
  (:use [clojure walk])
  (:use [backtype.storm util log])
  )

(def CLAIM-STRATEGY
  {:multi-threaded (fn [size] (MultiThreadedClaimStrategy. (int size)))
   :single-threaded (fn [size] (SingleThreadedClaimStrategy. (int size)))
    })
    
(def WAIT-STRATEGY
  {:block (fn [] (BlockingWaitStrategy.))
   :yield (fn [] (YieldingWaitStrategy.))
   :sleep (fn [] (SleepingWaitStrategy.))
   :spin (fn [] (BusySpinWaitStrategy.))
    })

(defnk disruptor-queue [buffer-size :claim-strategy :multi-threaded :wait-strategy :block]
  (DisruptorQueue. ((CLAIM-STRATEGY claim-strategy) buffer-size)
                   ((WAIT-STRATEGY wait-strategy))
                   ))

(defn to-halt-function [error-fn]
  (fn [t]
    (when (exception-cause? InterruptedException t)
      (log-message "Disruptor event handler interrupted")
      (throw t))
    (error-fn t)
    ))

(defnk set-handler [^DisruptorQueue queue handler-fn
                    :error-fn (fn [t] (log-error t) (halt-process! 1 "Error in transfer thread"))]
  (.setHandler queue
    (reify com.lmax.disruptor.EventHandler
      (onEvent [this o seq-id batchEnd?]
        (with-error-reaction (to-halt-function error-fn)
          (handler-fn o seq-id batchEnd?)
          )))))

(defn clojure-handler [afn]
  (reify com.lmax.disruptor.EventHandler
    (onEvent [this o seq-id batchEnd?]
      (afn o seq-id batchEnd?)
      )))

(defn non-blocking-disruptor-queue [buffer-size]
  (NonBlockingPairDisruptorQueue. buffer-size))

(defn consume-batch [^NonBlockingPairDisruptorQueue queue handler]
  (.consumeBatch queue handler))

(defmacro handler [& args]
  `(clojure-handler (fn ~@args)))

(defprotocol QueuePublish
  (publish [this o]))

(extend-protocol QueuePublish
  NonBlockingPairDisruptorQueue
  (publish [^NonBlockingPairDisruptorQueue this obj]
    (.publish this obj))
  DisruptorQueue
  (publish [^DisruptorQueue this obj]
    (.publish this obj)))
