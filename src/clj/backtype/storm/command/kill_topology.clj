(ns backtype.storm.command.kill-topology
  (:use [clojure.contrib.command-line :only [with-command-line]])
  (:use [backtype.storm thrift config log])
  (:import [backtype.storm.generated KillOptions])
  (:gen-class))


(defn -main [& args]
  (with-command-line args
    "Kill a topology"
    [[wait? w? "Override the amount of time to wait after deactivating before killing" nil]
     posargs]    
    (let [conf (read-storm-config)
          host (conf NIMBUS-HOST)
          port (conf NIMBUS-THRIFT-PORT)
          name (first posargs)
          opts (KillOptions.)]
      (if wait? (.set_wait_secs opts (Integer/parseInt wait?)))
      (with-nimbus-connection [nimbus host port]
        (.killTopologyWithOpts nimbus name opts)
        (log-message "Killed topology: " name)
        ))))
