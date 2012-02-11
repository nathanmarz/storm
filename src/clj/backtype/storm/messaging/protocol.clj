(ns backtype.storm.messaging.protocol
  (:refer-clojure :exclude [send])
  )

(defprotocol Connection
  (recv [conn])
  (send [conn task message])
  (close [conn])
  )

(defprotocol Context
  (bind [context storm-id virtual-port])
  (connect [context storm-id host port])
  (send-local-task-empty [context storm-id virtual-port])
  (term [context])
  )

