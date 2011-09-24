(ns backtype.storm.messaging.zmq
  (:refer-clojure :exclude [send])
  (:use [backtype.storm.messaging protocol])
  (:require [zilch.mq :as mq]
            [zilch.virtual-port :as mqvp]))

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


(defn mk-zmq-context [num-threads linger]
  (ZMQContext. (mq/context num-threads) linger))

