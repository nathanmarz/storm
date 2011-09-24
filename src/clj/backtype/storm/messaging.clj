(ns backtype.storm.messaging
  (:refer-clojure :exclude [send])
  (:import [java.util.concurrent LinkedBlockingQueue])
  (:require [zilch.mq :as mq]
            [zilch.virtual-port :as mqvp])
  (:use [backtype.storm util]))

;; TODO: Need to figure out a way to only load native libraries in zmq mode

(defprotocol Connection
  (recv [conn])
  (send [conn task message])
  )

(defprotocol Context
  (bind [context virtual-port])
  (connect [context host port])
  )

(deftype ZMQConnection [socket]
  Connection
  (recv [this]
    (mq/recv socket))
  (send [this task message]
    (mqvp/virtual-send socket task message)
    ))

(deftype ZMQContext [context linger-ms]
  Context
  (bind [this virtual-port]
    (-> context
        (mq/socket mq/pull)
        (mqvp/virtual-bind virtual-port)
        ))
  (connect [this host port]
    (let [url (str "tcp://" host ":" port)]
      (-> context
          (mq/socket mq/push)
          (mq/set-linger linger-ms)
          (mq/connect url))
      )))

(deftype LocalConnection [queues-map queue]
  Connection
  (recv [this]
    (when-not queue
      (throw (IllegalArgumentException. "Cannot receive on this socket")))
    (.take queue))
  (send [this task message]
    (let [send-queue (@queues-map task)]
      (.put send-queue message)
      )))

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
    ))


(defn mk-local-context []
  (LocalContext. (atom {}) (Object.)))

(defn mk-zmq-context [num-threads linger]
  (ZMQContext. (mq/context num-threads) linger))

