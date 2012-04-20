(ns backtype.storm.messaging.zmq
  (:refer-clojure :exclude [send])
  (:use [backtype.storm.messaging protocol])
  (:require [zilch.mq :as mq]
            [zilch.virtual-port :as mqvp]))

(defprotocol ZMQContextQuery
  (zmq-context [this]))

(deftype ZMQConnection [socket]
  Connection
  (recv [this]
    (mq/recv socket))
  (send [this task message]
    (mqvp/virtual-send socket task message))
  (close [this]
    (.close socket)
    ))

(deftype ZMQContext [context linger-ms ipc?]
  Context
  (bind [this storm-id port]
    (-> context
        (mq/socket mq/pull)
        (mqvp/virtual-bind port)
        (ZMQConnection.)
        ))
  (connect [this storm-id host port]
    (let [url (if ipc?
                (str "ipc://" port ".ipc")
                (str "tcp://" host ":" port))]
      (-> context
          (mq/socket mq/push)
          (mq/set-linger linger-ms)
          (mq/connect url)
          (ZMQConnection.))))
  (term [this]
    (.term context))
  ZMQContextQuery
  (zmq-context [this]
    context))


(defn mk-zmq-context [num-threads linger local?]
  (ZMQContext. (mq/context num-threads) linger local?))

