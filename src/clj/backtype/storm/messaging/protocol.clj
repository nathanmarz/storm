(ns backtype.storm.messaging.protocol
  (:refer-clojure :exclude [send])
  )

(defprotocol Connection
  (recv [conn])
  (send [conn task message])
  (close [conn])
  )

(defprotocol Context
  (bind [context virtual-port])
  (connect [context host port])
  (send-local-task-empty [context virtual-port])
  (term [context])
  )

