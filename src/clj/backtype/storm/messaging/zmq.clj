(ns backtype.storm.messaging.zmq
  (:refer-clojure :exclude [send])
  (:use [backtype.storm.messaging protocol])
  (:import [java.nio ByteBuffer])
  (:import [org.zeromq ZMQ])
  (:require [zilch.mq :as mq]))


(defn parse-packet [^bytes part1 ^bytes part2]
  (let [bb (ByteBuffer/wrap part1)
        port (.getShort bb)]
    [(int port) part2]
    ))

(defn get-bind-zmq-url [local? port]
  (if local?
    (str "ipc://" port ".ipc")
    (str "tcp://*:" port)))

(defn get-connect-zmq-url [local? host port]
  (if local?
    (str "ipc://" port ".ipc")
    (str "tcp://" host ":" port)))


(defprotocol ZMQContextQuery
  (zmq-context [this]))

(def NOBLOCK-SNDMORE (bit-or ZMQ/SNDMORE ZMQ/NOBLOCK))

(deftype ZMQConnection [socket ^ByteBuffer bb]
  Connection
  (recv-with-flags [this flags]
    (let [part1 (mq/recv socket flags)]
      (when part1
        (when-not (mq/recv-more? socket)
          (throw (RuntimeException. "Should always receive two-part ZMQ messages")))
        (parse-packet part1 (mq/recv socket)))))
  (send [this task message]
    (.clear bb)
    (.putShort bb (short task))
    (mq/send socket (.array bb) NOBLOCK-SNDMORE)
    (mq/send socket message ZMQ/NOBLOCK)) ;; TODO: how to do backpressure if doing noblock?... need to only unblock if the target disappears
  (close [this]
    (.close socket)
    ))

(defn mk-connection [socket]
  (ZMQConnection. socket (ByteBuffer/allocate 2)))

(deftype ZMQContext [context linger-ms hwm local?]
  Context
  (bind [this storm-id port]
    (-> context
        (mq/socket mq/pull)
        (mq/set-hwm hwm)
        (mq/bind (get-bind-zmq-url local? port))
        mk-connection
        ))
  (connect [this storm-id host port]
    (-> context
        (mq/socket mq/push)
        (mq/set-hwm hwm)
        (mq/set-linger linger-ms)
        (mq/connect (get-connect-zmq-url local? host port))
        mk-connection))
  (term [this]
    (.term context))
  ZMQContextQuery
  (zmq-context [this]
    context))

(defn mk-zmq-context [num-threads linger hwm local?]
  (ZMQContext. (mq/context num-threads) linger hwm local?))

