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
    [port msg]
    ))

(defn get-zmq-url [local? port]
  (if local?
    (str "ipc://" port ".ipc")
    (str "tcp://*:" port)))

(defprotocol ZMQContextQuery
  (zmq-context [this]))

(deftype ZMQConnection [socket]
  Connection
  (recv [this]
    (parse-packet (mq/recv socket)))
  (send [this task message]
    (mq/send socket (mk-packet task message) ZMQ/NOBLOCK))
  (close [this]
    (.close socket)
    ))

(deftype ZMQContext [context linger-ms local?]
  Context
  (bind [this storm-id port]
    (-> context
        (mq/socket mq/pull)
        (mq/bind (get-zmq-url local? port))
        (ZMQConnection.)
        ))
  (connect [this storm-id host port]
    (-> context
        (mq/socket mq/push)
        (mq/set-linger linger-ms)
        (mq/connect (get-zmq-url local? port))
        (ZMQConnection.)))
  (term [this]
    (.term context))
  ZMQContextQuery
  (zmq-context [this]
    context))

(defn mk-zmq-context [num-threads linger local?]
  (ZMQContext. (mq/context num-threads) linger local?))

