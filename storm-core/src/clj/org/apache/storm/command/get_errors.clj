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
(ns org.apache.storm.command.get-errors
  (:use [clojure.tools.cli :only [cli]])
  (:use [org.apache.storm thrift log])
  (:use [org.apache.storm util])
  (:require [org.apache.storm.daemon
             [nimbus :as nimbus]
             [common :as common]])
  (:import [org.apache.storm.generated GetInfoOptions NumErrorsChoice
            TopologySummary ErrorInfo])
  (:gen-class))

(defn get-topology-id [name topologies]
  (let [topology (first (filter #(= (.get_name %1) name) topologies))]
    (when (not-nil? topology) (.get_id topology))))

(defn get-component-errors
  [topology-errors]
  (apply hash-map (remove nil?
                    (flatten (for [[comp-name comp-errors] topology-errors]
                               (let [latest-error (when (not (empty? comp-errors)) (first comp-errors))]
                                 (if latest-error [comp-name (.get_error ^ErrorInfo latest-error)])))))))

(defn -main [name]
  (with-configured-nimbus-connection nimbus
    (let [opts (doto (GetInfoOptions.)
                 (.set_num_err_choice NumErrorsChoice/ONE))
          cluster-info (.getClusterInfo nimbus)
          topologies (.get_topologies cluster-info)
          topo-id (get-topology-id name topologies)
          topo-info (when (not-nil? topo-id) (.getTopologyInfoWithOpts nimbus topo-id opts))]
      (if (or (nil? topo-id) (nil? topo-info))
        (println (to-json {"Failure" (str "No topologies running with name " name)}))
        (let [topology-name (.get_name topo-info)
              topology-errors (.get_errors topo-info)]
          (println (to-json (hash-map
                              "Topology Name" topology-name
                              "Comp-Errors" (get-component-errors topology-errors)))))))))
