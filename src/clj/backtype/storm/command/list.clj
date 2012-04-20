(ns backtype.storm.command.list
  (:use [backtype.storm thrift log])
  (:import [backtype.storm.generated TopologySummary])
  (:gen-class))

(defn -main []
  (with-configured-nimbus-connection nimbus
    (let [cluster-info (.getClusterInfo nimbus)
          topologies (.get_topologies cluster-info)
          msg-format "%-20s %-10s %-10s %-12s %-10s"]
      (if (or (nil? topologies) (empty? topologies))
        (println "No topologies running.")
        (do
          (println (format msg-format "Topology_name" "Status" "Num_tasks" "Num_workers" "Uptime_secs"))
          (println "-------------------------------------------------------------------")
          (doseq [^TopologySummary topology topologies]
            (let [topology-name (.get_name topology)
                  topology-status (.get_status topology)
                  topology-num-tasks (.get_num_tasks topology)
                  topology-num-workers (.get_num_workers topology)
                  topology-uptime-secs (.get_uptime_secs topology)]
              (println (format msg-format  topology-name topology-status topology-num-tasks
                               topology-num-workers topology-uptime-secs)))))))))