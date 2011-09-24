(ns backtype.storm.messaging.loader  
  (:require [backtype.storm.messaging.local :as local]))

(defn mk-local-context []
  (local/mk-local-context))

(defn mk-zmq-context [num-threads linger]
  (require '[backtype.storm.messaging.zmq :as zmq])
  (let [afn (-> 'backtype.storm.messaging.zmq/mk-zmq-context
                find-var
                var-get)]
    (afn num-threads linger)))

(defn launch-virtual-port! [context & args]
  (require '[zilch.virtual-port :as mqvp])
  (require '[backtype.storm.messaging.zmq :as zmq])
  (let [afn (-> 'zilch.virtual-port/launch-virtual-port!
                find-var
                var-get)
        ]
    (apply afn (cons (.zmq-context context) args))))
