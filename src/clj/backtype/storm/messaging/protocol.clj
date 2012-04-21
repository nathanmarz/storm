(ns backtype.storm.messaging.protocol
  (:refer-clojure :exclude [send])
  )

(defprotocol Connection
  (recv [conn])
  (send [conn task message])
  (close [conn])
  )

(defprotocol Context
  (bind [context storm-id port])
  (connect [context storm-id host port])
  (term [context])
  )

