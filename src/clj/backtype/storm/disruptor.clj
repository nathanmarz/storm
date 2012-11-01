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


(defn- mk-wait-strategy [spec]
  (if (keyword? spec)
    ((WAIT-STRATEGY spec))
    (-> (str spec) new-instance)
    ))

;; :block strategy requires using a timeout on waitFor (implemented in DisruptorQueue), as sometimes the consumer stays blocked even when there's an item on the queue.
;; This would manifest itself in Trident when doing 1 batch at a time processing, and the ack_init message
;; wouldn't make it to the acker until the batch timed out and another tuple was played into the queue, 
;; unblocking the consumer
(defnk disruptor-queue [buffer-size :claim-strategy :multi-threaded :wait-strategy :block]
  (DisruptorQueue. ((CLAIM-STRATEGY claim-strategy) buffer-size)
                   (mk-wait-strategy wait-strategy)
                   ))

(defn clojure-handler [afn]
  (reify com.lmax.disruptor.EventHandler
    (onEvent [this o seq-id batchEnd?]
      (afn o seq-id batchEnd?)
      )))

(defmacro handler [& args]
  `(clojure-handler (fn ~@args)))

(defn publish
  ([^DisruptorQueue q o block?]
    (.publish q o block?))
  ([q o]
    (publish q o true)))

(defn try-publish [^DisruptorQueue q o]
  (.tryPublish q o))

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
              :kill-fn kill-fn
              )]
     (consumer-started! queue)
     ret
     ))

(defmacro consume-loop [queue & handler-args]
  `(let [handler# (handler ~@handler-args)]
     (consume-loop* ~queue handler#)
     ))
