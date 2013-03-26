(ns backtype.storm.messaging.zmq
  (:refer-clojure :exclude [send])
  (:use [backtype.storm config log])
  (:import [backtype.storm.messaging ITransport IContext IConnection TaskMessage])
  (:import [java.nio ByteBuffer])
  (:import [org.zeromq ZMQ])
  (:import [java.util Map])
  (:require [zilch.mq :as mq])
  (:gen-class))

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

(def NOBLOCK-SNDMORE (bit-or ZMQ/SNDMORE ZMQ/NOBLOCK))

(deftype ZMQConnection [socket ^ByteBuffer bb]
  IConnection
  (^TaskMessage recv [this ^int flags]
    (log-debug "ZMQConnection recv()")
    (require 'backtype.storm.messaging.zmq)
    (if-let [packet (mq/recv socket flags)]
      (parse-packet packet)))
  (^void send [this ^int taskId ^"[B" payload]
    (log-debug "ZMQConnection send()")
    (require 'backtype.storm.messaging.zmq)
    (mq/send socket (mk-packet taskId payload) ZMQ/NOBLOCK)) ;; TODO: how to do backpressure if doing noblock?... need to only unblock if the target disappears
  (^void close [this]
    (log-debug "ZMQConnection close()")
    (.close socket)))

(defn mk-connection [socket]
  (ZMQConnection. socket (ByteBuffer/allocate 2)))

(deftype ZMQContext [^{:volatile-mutable true} context 
                     ^{:volatile-mutable true} linger-ms 
                     ^{:volatile-mutable true} hwm 
                     ^{:volatile-mutable true} local?]
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

(deftype TransportPlugin [] 
  ITransport
  (^IContext newContext [this] 
    (ZMQContext. 0 0 0 true)))