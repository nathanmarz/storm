(ns backtype.storm.command.kill-topology
  (:use [clojure.tools.cli :only [cli]])
  (:use [backtype.storm thrift config log])
  (:import [backtype.storm.generated KillOptions])
  (:gen-class))

(defn -main [& args]
  (let [[{wait :wait} [name] _] (cli args ["-w" "--wait" :default nil :parse-fn #(Integer/parseInt %)])
        opts (KillOptions.)]
    (if wait (.set_wait_secs opts wait))
    (with-configured-nimbus-connection nimbus
      (.killTopologyWithOpts nimbus name opts)
      (log-message "Killed topology: " name)
      )))
