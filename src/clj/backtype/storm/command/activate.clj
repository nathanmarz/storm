(ns backtype.storm.command.activate
  (:use [backtype.storm thrift log])
  (:gen-class))

(defn -main [name] 
  (with-configured-nimbus-connection nimbus
    (.activate nimbus name)
    (log-message "Activated topology: " name)
    ))
