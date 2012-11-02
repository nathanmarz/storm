(ns backtype.storm.command.kill-topology
  (:use [clojure.tools.cli :only [cli]])
  (:use [backtype.storm thrift config log])
  (:import [backtype.storm.generated KillOptions])
  (:gen-class))

(defn kill-topology [nimbus name opts]
  (.killTopologyWithOpts nimbus name opts)
  (log-message "Killed topology: " name))

(defn -main [& args]
  (let [[{wait :wait all :all} [name] _]
        (cli args ["-w" "--wait" :default nil :parse-fn #(Integer/parseInt %)]
                  ["-a" "--all" :flag true])
        opts (KillOptions.)]
    (if wait (.set_wait_secs opts wait))
    (with-configured-nimbus-connection nimbus
      (if name
        (kill-topology nimbus name opts)
        (do
          (log-message "Killing all topologies...")
          (doseq [topology (.get_topologies (.getClusterInfo nimbus))]
            (let [name (.get_name topology)]
              (kill-topology nimbus name opts)))
          (log-message "All topologies killed")
          ))
      )))
