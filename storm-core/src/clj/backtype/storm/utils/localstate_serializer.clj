;; Use to serialize LocalState. We assume that LocalState is a k/v store that
;; uses java.util.HashMap to store the following keys
;;    LS_ID : String
;;    LS_WORKER_HEARTBEAT : backtype.storm.daemon.common.WorkerHeartbeat
;;    LS_LOCAL_ASSIGNMENTS : clojure.lang.PersistentArrayMap
;;    LS_APPROVED_WORKERS : clojure.lang.PersistentArrayMap

(ns backtype.storm.utils.localstate-serializer
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm Constants])
  (:use [backtype.storm util])
  )

; java.util.HashMap -> byte[]
(defn serialize-localstate [form]
  (serialize-clj-bytes (into {} form)))

; byte[] -> java.util.HashMap
(defn deserialize-localstate [form]
  (let [newm (java.util.HashMap.)]
    (.putAll newm (deserialize-clj-bytes form))
    newm))

(defn localstate-serializer []
  (reify
    backtype.storm.utils.StateSerializer
    (serializeState [this val] (serialize-localstate val))
    (deserializeState [this ser] (deserialize-localstate ser))))

(defn localstate-default-serializer []
  (reify
    backtype.storm.utils.StateSerializer
    (serializeState [this val] (Utils/serialize val))
    (deserializeState [this ser] (Utils/deserialize ser))))