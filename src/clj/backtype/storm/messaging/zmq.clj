(ns backtype.storm.messaging.zmq
  (:refer-clojure :exclude [send])
  (:use [backtype.storm config log])
  (:import [backtype.storm.messaging IContext IConnection TaskMessage])
  (:import [java.nio ByteBuffer])
  (:import [org.zeromq ZMQ])
  (:import [java.util Map])
  (:require [zilch.mq :as mq])
  (:gen-class
    :methods [^{:static true} [makeContext [java.util.Map] backtype.storm.messaging.IContext]]))

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
    (TaskMessage. (int port) msg)
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
  IConnection
  (^TaskMessage recv [this ^int flags]
    (require 'backtype.storm.messaging.zmq)
    (if-let [packet (mq/recv socket flags)]
      (parse-packet packet)))
  (^void send [this ^int taskId ^bytes payload]
    (require 'backtype.storm.messaging.zmq)
    (mq/send socket (mk-packet taskId payload) ZMQ/NOBLOCK)) ;; TODO: how to do backpressure if doing noblock?... need to only unblock if the target disappears
  (^void close [this]
    (.close socket)))

(defn mk-connection [socket]
  (ZMQConnection. socket))

(deftype ZMQContext [^{:unsynchronized-mutable true} context 
                     ^{:unsynchronized-mutable true} linger-ms 
                     ^{:unsynchronized-mutable true} hwm 
                     ^{:unsynchronized-mutable true} local?]
  IContext
  (^void prepare [this ^Map storm-conf]
    (let [num-threads (storm-conf ZMQ-THREADS)]
      (set! context (mq/context num-threads)) 
      (set! linger-ms (storm-conf ZMQ-LINGER-MILLIS))
      (set! hwm (storm-conf ZMQ-HWM))
      (set! local? (= (storm-conf STORM-CLUSTER-MODE) "local"))))
  (^IConnection bind [this ^String storm-id ^int port]
    (require 'backtype.storm.messaging.zmq)
    (-> context
      (mq/socket mq/pull)
      (mq/set-hwm hwm)
      (mq/bind (get-bind-zmq-url local? port))
      mk-connection
      ))
  (^IConnection connect [this ^String storm-id ^String host ^int port]
    (require 'backtype.storm.messaging.zmq)
    (-> context
      (mq/socket mq/push)
      (mq/set-hwm hwm)
      (mq/set-linger linger-ms)
      (mq/connect (get-connect-zmq-url local? host port))
      mk-connection))
  (^void term [this]
    (.term context))
  
  ZMQContextQuery
  (zmq-context [this]
    context))

(defn -makeContext [^Map storm-conf] 
  (let [context (ZMQContext. nil 0 0 true)]
    (.prepare context storm-conf)
    context))
