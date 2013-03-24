(ns backtype.storm.messaging.local
  (:refer-clojure :exclude [send])
  (:use [backtype.storm log])
  (:import [backtype.storm.messaging IContext IConnection TaskMessage])
  (:import [java.util.concurrent LinkedBlockingQueue])
  (:import [java.util Map])
  )

(defn add-queue! [queues-map lock storm-id port]
  (let [id (str storm-id "-" port)]
    (locking lock
      (when-not (contains? @queues-map id)
        (swap! queues-map assoc id (LinkedBlockingQueue.))))
    (@queues-map id)))

(deftype LocalConnection [storm-id port queues-map lock queue]
  IConnection
  (^TaskMessage recv [this]
    (.recv-with-flags this 0))
  (^TaskMessage recv-with-flags [this ^int flags]
    (when-not queue
      (throw (IllegalArgumentException. "Cannot receive on this socket")))
    (if (= flags 1)
      (.poll queue)
      (.take queue)))
  (^void send [this ^int task ^"[B" message]
    (log-message "LocalConnection: task:" task " storm-id:" storm-id)
    (let [send-queue (add-queue! queues-map lock storm-id port)]
      (log-message ".put task:" task " message:" message)
      (.put send-queue (TaskMessage. task message))
      ))
  (^void close [this]
    ))


(deftype LocalContext [^{:volatile-mutable true} queues-map ^{:volatile-mutable true} lock]
  IContext
  (^void prepare [this ^Map storm-conf]
    (set! queues-map (atom {}))
    (set! lock (Object.))
    )
  (^IConnection bind [this ^String storm-id ^int port]
    (LocalConnection. storm-id port queues-map lock (add-queue! queues-map lock storm-id port)))
  (^IConnection connect [this ^String storm-id ^String host ^int port]
    (LocalConnection. storm-id port queues-map lock nil)
    )
  (^void term [this]
    ))

(defn mk-local-context []
  (let [context (LocalContext. nil nil)]
    (.prepare context nil)
    context))
