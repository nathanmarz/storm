(ns backtype.storm.messaging.local
  (:refer-clojure :exclude [send])
  (:use [backtype.storm.messaging protocol])
  (:import [java.util.concurrent LinkedBlockingQueue])
  )

(deftype LocalConnection [queues-map queue]
  Connection
  (recv [this]
    (when-not queue
      (throw (IllegalArgumentException. "Cannot receive on this socket")))
    (.take queue))
  (send [this task message]
    (let [send-queue (@queues-map task)]
      (.put send-queue message)
      ))
  (close [this]
    ))

(defn add-queue! [queues-map lock port]
  (locking lock
    (if-not (contains? @queues-map port)
      (swap! queues-map assoc port (LinkedBlockingQueue.)))))

(deftype LocalContext [queues-map lock]
  Context
  (bind [this virtual-port]
    (LocalConnection. queues-map (add-queue! queues-map lock virtual-port)))
  (connect [this host port]
    (LocalConnection. queues-map nil)
    )
  (send-local-task-empty [this virtual-port]
    (let [queue (add-queue! queues-map lock virtual-port)]
      (.put queue (byte-array []))
      ))
  (term [this]
    ))

(defn mk-local-context []
  (LocalContext. (atom {}) (Object.)))
