(ns backtype.storm.command.kill-topology
  (:use [clojure.contrib.command-line :only [with-command-line]])
  (:use [backtype.storm thrift config log])
  (:import [backtype.storm.generated KillOptions])
  (:gen-class))


(defn -main [& args]
  (with-command-line args
    "Kill a topology"
    [[wait w "Override the amount of time to wait after deactivating before killing" nil]
     posargs]    
    (let [name (first posargs)
          opts (KillOptions.)]
      (if wait (.set_wait_secs opts (Integer/parseInt wait)))
      (with-configured-nimbus-connection nimbus
        (.killTopologyWithOpts nimbus name opts)
        (log-message "Killed topology: " name)
        ))))
