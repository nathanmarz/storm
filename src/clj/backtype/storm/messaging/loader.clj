(ns backtype.storm.messaging.loader
  (:use [backtype.storm util log])
  (:import [java.util.concurrent LinkedBlockingQueue])
  (:require [backtype.storm.messaging [local :as local] [protocol :as msg]]))

(defn mk-local-context []
  (local/mk-local-context))

(defn mk-zmq-context [& args]
  (require '[backtype.storm.messaging.zmq :as zmq])
  (let [afn (-> 'backtype.storm.messaging.zmq/mk-zmq-context
                find-var
                var-get)]
    (apply afn args)))

(defnk launch-receive-thread!
  [context storm-id port receive-queue-map
   :daemon true
   :kill-fn (fn [t] (System/exit 1))
   :priority Thread/NORM_PRIORITY]
  (let [vthread (async-loop
                 (fn [socket receive-queue-map]
                   (let [[task msg] (msg/recv socket)]
                     (if (= task -1)
                       (do
                         (log-message "Receiving-thread:[" storm-id ", " port "] received shutdown notice")
                         (.close socket)
                         nil )
                       (do
                         (if (contains? receive-queue-map task)
                           (.put ^LinkedBlockingQueue (receive-queue-map task) msg)
                           (log-message "Receiving-thread:[" storm-id ", " port "] received invalid message for unknown task " task ". Dropping..."))
                          0 ))))
                 :args-fn (fn [] [(msg/bind context storm-id port) receive-queue-map])
                 :daemon daemon
                 :kill-fn kill-fn
                 :priority priority)]
    (fn []
      (let [kill-socket (msg/connect context storm-id "localhost" port)]
        (log-message "Shutting down receiving-thread: [" storm-id ", " port "]")
        (msg/send kill-socket
                  -1
                  (byte-array []))
        (log-message "Waiting for receiving-thread:[" storm-id ", " port "] to die")
        (.join vthread)
        (.close kill-socket)
        (log-message "Shutdown receiving-thread: [" storm-id ", " port "]")
        ))))


