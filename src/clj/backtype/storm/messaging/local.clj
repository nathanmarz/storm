(ns backtype.storm.messaging.local
  (:refer-clojure :exclude [send])
  (:use [backtype.storm.messaging protocol])
  (:import [java.util.concurrent LinkedBlockingQueue])
  )

(defn add-queue! [queues-map lock storm-id port]
  (let [id (str storm-id "-" port)]
    (locking lock
      (when-not (contains? @queues-map id)
        (swap! queues-map assoc id (LinkedBlockingQueue.))))
    (@queues-map id)))

(deftype LocalConnection [storm-id port queues-map lock queue]
  Connection
  (recv-with-flags [this flags]
    (when-not queue
      (throw (IllegalArgumentException. "Cannot receive on this socket")))
    (if (= flags 1)
      (.poll queue)
      (.take queue)))
  (send [this task message]
    (let [send-queue (add-queue! queues-map lock storm-id port)]
      (.put send-queue [task message])
      ))
  (close [this]
    ))


(deftype LocalContext [queues-map lock]
  Context
  (bind [this storm-id port]
    (LocalConnection. storm-id port queues-map lock (add-queue! queues-map lock storm-id port)))
  (connect [this storm-id host port]
    (LocalConnection. storm-id port queues-map lock nil)
    )
  (term [this]
    ))

(defn mk-local-context []
  (LocalContext. (atom {}) (Object.)))
