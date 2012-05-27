(ns backtype.storm.disruptor
  (:import [backtype.storm.utils DisruptorQueue])
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

(defn clojure-handler [afn]
  (reify com.lmax.disruptor.EventHandler
    (onEvent [this o seq-id batchEnd?]
      (afn o seq-id batchEnd?)
      )))

(defmacro handler [& args]
  `(clojure-handler (fn ~@args)))

(defn publish [^DisruptorQueue q o]
  (.publish q o))

(defn consume-batch [^DisruptorQueue queue handler]
  (.consumeBatch queue handler))

(defn consume-batch-when-available [^DisruptorQueue queue handler]
  (.consumeBatchWhenAvailable queue handler))

(defn consumer-started! [^DisruptorQueue queue]
  (.consumerStarted queue))

(defn halt-with-interrupt! [^DisruptorQueue queue]
  (.haltWithInterrupt queue))

(defnk consume-loop* [^DisruptorQueue queue handler :kill-fn (fn [error] (halt-process! 1 "Async loop died!"))]
  (let [ret (async-loop
              (fn []
                (consume-batch-when-available queue handler)
                0 )
              :kill-fn kill-fn)]
     (consumer-started! queue)
     ret
     ))

(defmacro consume-loop [queue & handler-args]
  `(let [handler# (handler ~@handler-args)]
     (consume-loop* ~queue handler#)
     ))
