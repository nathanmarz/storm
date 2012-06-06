(ns backtype.storm.messaging.loader
  (:use [backtype.storm util log])
  (:import [java.util ArrayList])
  (:import [backtype.storm.utils DisruptorQueue MutableObject])
  (:require [backtype.storm.messaging [local :as local] [protocol :as msg]])
  (:require [backtype.storm [disruptor :as disruptor]]))

(defn mk-local-context []
  (local/mk-local-context))

(defn mk-zmq-context [& args]
  (require '[backtype.storm.messaging.zmq :as zmq])
  (let [afn (-> 'backtype.storm.messaging.zmq/mk-zmq-context
                find-var
                var-get)]
    (apply afn args)))

(defnk launch-receive-thread!
  [context storm-id port transfer-local-fn max-buffer-size
   :daemon true
   :kill-fn (fn [t] (System/exit 1))
   :priority Thread/NORM_PRIORITY]
  (let [max-buffer-size (int max-buffer-size)
        vthread (async-loop
                 (fn []
                   (let [socket (msg/bind context storm-id port)]
                     (fn []
                       (let [batched (ArrayList.)
                             init (msg/recv socket)]
                         (loop [[task msg :as packet] init]
                           (if (= task -1)
                             (do (log-message "Receiving-thread:[" storm-id ", " port "] received shutdown notice")
                                 (.close socket)
                                 nil )
                             (do
                               (when packet (.add batched packet))
                               (if (and packet (< (.size batched) max-buffer-size))
                                 (recur (msg/recv-with-flags socket 1))
                                 (do (transfer-local-fn batched)
                                     0 )))))))))
                 :factory? true
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


