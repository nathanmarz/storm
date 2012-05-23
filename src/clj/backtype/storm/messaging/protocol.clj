(ns backtype.storm.messaging.protocol
  (:refer-clojure :exclude [send])
  )

(defprotocol Connection
  (recv-with-flags [conn flags])
  (send [conn task message])
  (close [conn])
  )

(defprotocol Context
  (bind [context storm-id port])
  (connect [context storm-id host port])
  (term [context])
  )

(defn recv [conn]
  (recv-with-flags conn 0))

