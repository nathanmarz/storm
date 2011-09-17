(ns backtype.storm.command.kill-topology
  (:use [backtype.storm thrift config log])
  (:gen-class))


(defn -main [^String name]
  (let [conf (read-storm-config)
        host (conf NIMBUS-HOST)
        port (conf NIMBUS-THRIFT-PORT)]
    (with-nimbus-connection [nimbus host port]
      (.killTopology nimbus name)
      (log-message "Killed storm: " name)
      )))
