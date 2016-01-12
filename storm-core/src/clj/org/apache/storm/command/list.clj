;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns org.apache.storm.command.list
  (:use [org.apache.storm thrift log])
  (:import [org.apache.storm.generated TopologySummary])
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
