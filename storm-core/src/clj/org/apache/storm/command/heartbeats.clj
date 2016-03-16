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
(ns org.apache.storm.command.heartbeats
  (:require [org.apache.storm
             [config :refer :all]
             [log :refer :all]
             [util :refer :all]
             [converter :refer :all]]
            [clojure.string :as string])
  (:import [org.apache.storm.generated ClusterWorkerHeartbeat]
           [org.apache.storm.utils Utils ConfigUtils]
           [org.apache.storm.cluster ZKStateStorage ClusterStateContext ClusterUtils]
           [org.apache.storm.stats StatsUtil])
  (:gen-class))

(defn -main [command path & args]
  (let [conf (clojurify-structure (ConfigUtils/readStormConfig))
        cluster (ClusterUtils/mkStateStorage conf conf nil (ClusterStateContext.))]
    (println "Command: [" command "]")
    (condp = command
      "list"
      (let [message (clojure.string/join " \n" (.get_worker_hb_children cluster path false))]
        (log-message "list " path ":\n"
                     message "\n"))
      "get"
      (log-message 
       (if-let [hb (.get_worker_hb cluster path false)]
         (StatsUtil/convertZkWorkerHb
          (Utils/deserialize
           hb
           ClusterWorkerHeartbeat))
         "Nothing"))
      
      (log-message "Usage: heartbeats [list|get] path"))
    
    (try
      (.close cluster)
      (catch Exception e
        (log-message "Caught exception: " e " on close."))))
  (System/exit 0))
         
