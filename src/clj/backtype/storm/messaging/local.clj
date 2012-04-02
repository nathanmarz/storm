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

(deftype LocalConnection [storm-id queues-map lock queue]
  Connection
  (recv [this]
    (when-not queue
      (throw (IllegalArgumentException. "Cannot receive on this socket")))
    (.take queue))
  (send [this task message]
    (let [send-queue (add-queue! queues-map lock storm-id task)]
      (.put send-queue message)
      ))
  (close [this]
    ))


(deftype LocalContext [queues-map lock]
  Context
  (bind [this storm-id virtual-port]
    (LocalConnection. storm-id queues-map lock (add-queue! queues-map lock storm-id virtual-port)))
  (connect [this storm-id host port]
    (LocalConnection. storm-id queues-map lock nil)
    )
  (send-local-task-empty [this storm-id virtual-port]
    (let [queue (add-queue! queues-map lock storm-id virtual-port)]
      (.put queue (byte-array []))
      ))
  (term [this]
    ))

(defn mk-local-context []
  (LocalContext. (atom {}) (Object.)))
