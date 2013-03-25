(ns backtype.storm.messaging.zmq
  (:refer-clojure :exclude [send])
  (:use [backtype.storm.messaging protocol])
  (:import [java.nio ByteBuffer])
  (:import [org.zeromq ZMQ])
  (:require [zilch.mq :as mq]))


(defn mk-packet [task ^bytes message]
  (let [bb (ByteBuffer/allocate (+ 2 (count message)))]
    (.putShort bb (short task))
    (.put bb message)
    (.array bb)
    ))

(defn parse-packet [^bytes packet]
  (let [bb (ByteBuffer/wrap packet)
        port (.getShort bb)
        msg (byte-array (- (count packet) 2))]
    (.get bb msg)
    [(int port) msg]
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

(deftype ZMQConnection [socket]
  Connection
  (recv-with-flags [this flags]
    (if-let [packet (mq/recv socket flags)]
      (parse-packet packet)))
  (send [this task message]
    (mq/send socket (mk-packet task message) ZMQ/NOBLOCK)) ;; TODO: how to do backpressure if doing noblock?... need to only unblock if the target disappears
  (close [this]
    (.close socket)
    ))

(defn mk-connection [socket]
  (ZMQConnection. socket))

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

