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
  [context storm-id port transfer-local-fn send-buffer-size
   :daemon true
   :kill-fn (fn [t] (System/exit 1))
   :priority Thread/NORM_PRIORITY]
  (let [receive-batcher (disruptor/disruptor-queue send-buffer-size :claim-strategy :single-threaded)
        cached-emit (MutableObject. (ArrayList.))
        vthread (async-loop
                 (fn [socket]
                   (let [[task msg :as packet] (msg/recv socket)]
                     (if (= task -1)
                       (do
                         (log-message "Receiving-thread:[" storm-id ", " port "] received shutdown notice")
                         (.close socket)
                         nil )
                       (do
                         (disruptor/publish receive-batcher packet)
                         0 ))))
                 :args-fn (fn [] [(msg/bind context storm-id port)])
                 :daemon daemon
                 :kill-fn kill-fn
                 :priority priority)]
    (disruptor/set-handler
      receive-batcher
      (fn [o seq-id batch-end?]
        (let [^ArrayList alist (.getObject cached-emit)]
          (.add alist o)
          (when batch-end?
            (transfer-local-fn alist)
            (.setObject cached-emit (ArrayList.))
            ))))
    (fn []
      (let [kill-socket (msg/connect context storm-id "localhost" port)]
        (log-message "Shutting down receiving-thread: [" storm-id ", " port "]")
        (msg/send kill-socket
                  -1
                  (byte-array []))
        (log-message "Waiting for receiving-thread:[" storm-id ", " port "] to die")
        (.join vthread)
        (.shutdown receive-batcher)
        (.close kill-socket)
        (log-message "Shutdown receiving-thread: [" storm-id ", " port "]")
        ))))


