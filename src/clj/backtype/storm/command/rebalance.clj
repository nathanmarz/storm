(ns backtype.storm.command.rebalance
  (:use [clojure.tools.cli :only [cli]])
  (:use [backtype.storm thrift config log])
  (:import [backtype.storm.generated RebalanceOptions])
  (:gen-class))

(defn -main [& args] 
  (let [[{wait :wait} [name] _] (cli args ["-w" "--wait" :default nil :parse-fn #(Integer/parseInt %)])
        opts (RebalanceOptions.)]
    (if wait (.set_wait_secs opts wait))
    (with-configured-nimbus-connection nimbus
      (.rebalance nimbus name opts)
      (log-message "Topology " name " is rebalancing")
      )))
