(ns backtype.storm.command.deactivate
  (:use [backtype.storm thrift log])
  (:gen-class))

(defn -main [name] 
  (with-configured-nimbus-connection nimbus
    (.deactivate nimbus name)
    (log-message "Deactivated topology: " name)
    ))
