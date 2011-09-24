(ns backtype.storm.messaging.protocol
  (:refer-clojure :exclude [send])
  )

(defprotocol Connection
  (recv [conn])
  (send [conn task message])
  )

(defprotocol Context
  (bind [context virtual-port])
  (connect [context host port])
  )

