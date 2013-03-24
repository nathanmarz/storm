(ns backtype.storm.messaging.zmq
  (:refer-clojure :exclude [send])
  (:use [backtype.storm config log])
  (:import [backtype.storm.messaging ITransport IContext IConnection TaskMessage])
  (:import [java.nio ByteBuffer])
  (:import [org.zeromq ZMQ])
  (:import [java.util Map])
  (:require [zilch.mq :as mq])
  (:gen-class))

(defn parse-packet [^bytes part1 ^bytes part2]
  (let [bb (ByteBuffer/wrap part1)
        port (.getShort bb)]
    (TaskMessage. (int port) part2)
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
  (^TaskMessage recv [this]
    (.recv-with-flags this 0))
  (^TaskMessage recv-with-flags [this ^int flags]
    (let [part1 (mq/recv socket flags)]
      (when part1
        (when-not (mq/recv-more? socket)
          (throw (RuntimeException. "Should always receive two-part ZMQ messages")))
        (parse-packet part1 (mq/recv socket)))))
  (^void send [this ^int task ^"[B" message]
    (log-message "ZMQConnection task:" task " socket:" socket)
    (.clear bb)
    (.putShort bb (short task))
    (mq/send socket (.array bb) NOBLOCK-SNDMORE)
    (mq/send socket message ZMQ/NOBLOCK)) ;; TODO: how to do backpressure if doing noblock?... need to only unblock if the target disappears
  (^void close [this]
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
    (-> context
      (mq/socket mq/pull)
      (mq/set-hwm hwm)
      (mq/bind (get-bind-zmq-url local? port))
      mk-connection
      ))
  (^IConnection connect [this ^String storm-id ^String host ^int port]
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