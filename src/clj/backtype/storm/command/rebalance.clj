(ns backtype.storm.command.rebalance
  (:use [clojure.contrib.command-line :only [with-command-line]])
  (:use [backtype.storm thrift config log])
  (:import [backtype.storm.generated RebalanceOptions])
  (:gen-class))


(defn -main [& args]
  (with-command-line args
    "Rebalance a topology"
    [[wait w "Override the amount of time to wait after deactivating before rebalancing" nil]
     posargs]    
    (let [name (first posargs)
          opts (RebalanceOptions.)]
      (if wait (.set_wait_secs opts (Integer/parseInt wait)))
      (with-configured-nimbus-connection nimbus
        (.rebalance nimbus name opts)
        (log-message "Topology " name " is rebalancing")
        ))))
