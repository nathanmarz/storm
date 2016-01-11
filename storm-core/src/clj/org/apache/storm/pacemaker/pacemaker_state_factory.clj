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

(ns org.apache.storm.pacemaker.pacemaker-state-factory
  (:require [org.apache.storm.pacemaker pacemaker]
            [org.apache.storm.cluster-state [zookeeper-state-factory :as zk-factory]]
            [org.apache.storm
             [config :refer :all]
             [cluster :refer :all]
             [log :refer :all]
             [util :as util]])
  (:import [org.apache.storm.generated
            HBExecutionException HBServerMessageType HBMessage
            HBMessageData HBPulse]
           [org.apache.storm.cluster_state zookeeper_state_factory]
           [org.apache.storm.cluster ClusterState]
           [org.apache.storm.pacemaker PacemakerClient])
  (:gen-class
   :implements [org.apache.storm.cluster.ClusterStateFactory]))

;; So we can mock the client for testing
(defn makeClient [conf]
  (PacemakerClient. conf))

(defn makeZKState [conf auth-conf acls context]
  (.mkState (zookeeper_state_factory.) conf auth-conf acls context))

(def max-retries 10)

(defn -mkState [this conf auth-conf acls context]
  (let [zk-state (makeZKState conf auth-conf acls context)
        pacemaker-client (makeClient conf)]

    (reify
      ClusterState
      ;; Let these pass through to the zk-state. We only want to handle heartbeats.
      (register [this callback] (.register zk-state callback))
      (unregister [this callback] (.unregister zk-state callback))
      (set_ephemeral_node [this path data acls] (.set_ephemeral_node zk-state path data acls))
      (create_sequential [this path data acls] (.create_sequential zk-state path data acls))
      (set_data [this path data acls] (.set_data zk-state path data acls))
      (delete_node [this path] (.delete_node zk-state path))
      (delete_node_blobstore [this path nimbus-host-port-info] (.delete_node_blobstore zk-state path nimbus-host-port-info))
      (get_data [this path watch?] (.get_data zk-state path watch?))
      (get_data_with_version [this path watch?] (.get_data_with_version zk-state path watch?))
      (get_version [this path watch?] (.get_version zk-state path watch?))
      (get_children [this path watch?] (.get_children zk-state path watch?))
      (mkdirs [this path acls] (.mkdirs zk-state path acls))
      (node_exists [this path watch?] (.node_exists zk-state path watch?))
      (add_listener [this listener] (.add_listener zk-state listener))
      (sync_path [this path] (.sync_path zk-state path))
      
      (set_worker_hb [this path data acls]
        (util/retry-on-exception
         max-retries
         "set_worker_hb"
         #(let [response
                (.send pacemaker-client
                       (HBMessage. HBServerMessageType/SEND_PULSE
                                   (HBMessageData/pulse
                                    (doto (HBPulse.)
                                      (.set_id path)
                                      (.set_details data)))))]
            (if (= (.get_type response) HBServerMessageType/SEND_PULSE_RESPONSE)
              :ok
              (throw (HBExecutionException. "Invalid Response Type"))))))

      (delete_worker_hb [this path]
        (util/retry-on-exception
         max-retries
         "delete_worker_hb"
         #(let [response
                (.send pacemaker-client
                       (HBMessage. HBServerMessageType/DELETE_PATH
                                   (HBMessageData/path path)))]
            (if (= (.get_type response) HBServerMessageType/DELETE_PATH_RESPONSE)
              :ok
              (throw (HBExecutionException. "Invalid Response Type"))))))
      
      (get_worker_hb [this path watch?]
        (util/retry-on-exception
         max-retries
         "get_worker_hb"
         #(let [response
                (.send pacemaker-client
                       (HBMessage. HBServerMessageType/GET_PULSE
                                   (HBMessageData/path path)))]
            (if (= (.get_type response) HBServerMessageType/GET_PULSE_RESPONSE)
              (try 
                (.get_details (.get_pulse (.get_data response)))
                (catch Exception e
                  (throw (HBExecutionException. (.toString e)))))
              (throw (HBExecutionException. "Invalid Response Type"))))))
      
      (get_worker_hb_children [this path watch?]
        (util/retry-on-exception
         max-retries
         "get_worker_hb_children"
         #(let [response
                (.send pacemaker-client
                       (HBMessage. HBServerMessageType/GET_ALL_NODES_FOR_PATH
                                   (HBMessageData/path path)))]
            (if (= (.get_type response) HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE)
              (try
                (into [] (.get_pulseIds (.get_nodes (.get_data response))))
                (catch Exception e
                  (throw (HBExecutionException. (.toString e)))))
              (throw (HBExecutionException. "Invalid Response Type"))))))
      
      (close [this]
        (.close zk-state)
        (.close pacemaker-client)))))
