(ns backtype.storm.command.rebalance
  (:use [clojure.tools.cli :only [cli]])
  (:use [backtype.storm thrift config log])
  (:import [backtype.storm.generated RebalanceOptions])
  (:gen-class))

(defn- parse-executor [^String s]
  (let [eq-pos (.lastIndexOf s "=")
        name (.substring s 0 eq-pos)
        amt (.substring s (inc eq-pos))]
    {name (Integer/parseInt amt)}
    ))

(defn -main [& args] 
  (let [[{wait :wait executor :executor num-workers :num-workers} [name] _]
                  (cli args ["-w" "--wait" :default nil :parse-fn #(Integer/parseInt %)]
                            ["-n" "--num-workers" :default nil :parse-fn #(Integer/parseInt %)]
                            ["-e" "--executor" :combine-fn merge :parse-fn parse-executor])
        opts (RebalanceOptions.)]
    (if wait (.set_wait_secs opts wait))
    (if executor (.set_num_executors opts executor))
    (if num-workers (.set_num_workers opts num-workers))
    (with-configured-nimbus-connection nimbus
      (.rebalance nimbus name opts)
      (log-message "Topology " name " is rebalancing")
      )))
