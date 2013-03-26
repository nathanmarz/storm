(ns backtype.storm.messaging.local
  (:refer-clojure :exclude [send])
  (:use [backtype.storm log])
  (:import [backtype.storm.messaging ITransport IContext IConnection TaskMessage])
  (:import [java.util.concurrent LinkedBlockingQueue])
  (:import [java.util Map])
  (:gen-class))

(defn add-queue! [queues-map lock storm-id port]
  (let [id (str storm-id "-" port)]
    (locking lock
      (when-not (contains? @queues-map id)
        (swap! queues-map assoc id (LinkedBlockingQueue.))))
    (@queues-map id)))

(deftype LocalConnection [storm-id port queues-map lock queue]
  IConnection
  (^TaskMessage recv [this ^int flags]
    (log-debug "LocalConnection recv()")
    (when-not queue
      (throw (IllegalArgumentException. "Cannot receive on this socket")))
    (if (= flags 1)
      (.poll queue)
      (.take queue)))
  (^void send [this ^int taskId ^"[B" payload]
    (log-debug "LocalConnection send()")
    (let [send-queue (add-queue! queues-map lock storm-id port)]
      (.put send-queue (TaskMessage. taskId payload))
      ))
  (^void close [this]
    (log-debug "LocalConnection close()")
    ))


(deftype LocalContext [^{:volatile-mutable true} queues-map
                       ^{:volatile-mutable true} lock]
  IContext
  (^void prepare [this ^Map storm-conf]
    (set! queues-map (atom {}))
    (set! lock (Object.)))
  (^IConnection bind [this ^String storm-id ^int port]
    (LocalConnection. storm-id port queues-map lock (add-queue! queues-map lock storm-id port)))
  (^IConnection connect [this ^String storm-id ^String host ^int port]
    (LocalConnection. storm-id port queues-map lock nil))
  (^void term [this]
    ))

(defn mk-context [] 
  (let [context  (LocalContext. nil nil)]
    (.prepare ^IContext context nil)
    context))