(ns backtype.storm.messaging.loader  
  (:require [backtype.storm.messaging.local :as local]))

(defn mk-local-context []
  (local/mk-local-context))

(defn mk-zmq-context [& args]
  (require '[backtype.storm.messaging.zmq :as zmq])
  (let [afn (-> 'backtype.storm.messaging.zmq/mk-zmq-context
                find-var
                var-get)]
    (apply afn args)))

(defn launch-virtual-port! [local? context port & args]
  (require '[zilch.virtual-port :as mqvp])
  (require '[backtype.storm.messaging.zmq :as zmq])
  (let [afn (-> 'zilch.virtual-port/launch-virtual-port!
                find-var
                var-get)
        url (if local?
              (str "ipc://" port ".ipc")
              (str "tcp://*:" port))
        ]
    (apply afn (concat [(.zmq-context context) url] args))))
