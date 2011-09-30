(ns backtype.storm.messaging.local
  (:refer-clojure :exclude [send])
  (:use [backtype.storm.messaging protocol])
  (:import [java.util.concurrent LinkedBlockingQueue])
  )

(defn add-queue! [queues-map lock port]  
  (locking lock
    (when-not (contains? @queues-map port)
      (swap! queues-map assoc port (LinkedBlockingQueue.))))
  (@queues-map port))

(deftype LocalConnection [queues-map lock queue]
  Connection
  (recv [this]
    (when-not queue
      (throw (IllegalArgumentException. "Cannot receive on this socket")))
    (.take queue))
  (send [this task message]
    (let [send-queue (add-queue! queues-map lock task)]
      (.put send-queue message)
      ))
  (close [this]
    ))


(deftype LocalContext [queues-map lock]
  Context
  (bind [this virtual-port]
    (LocalConnection. queues-map lock (add-queue! queues-map lock virtual-port)))
  (connect [this host port]
    (LocalConnection. queues-map lock nil)
    )
  (send-local-task-empty [this virtual-port]
    (let [queue (add-queue! queues-map lock virtual-port)]
      (.put queue (byte-array []))
      ))
  (term [this]
    ))

(defn mk-local-context []
  (LocalContext. (atom {}) (Object.)))
