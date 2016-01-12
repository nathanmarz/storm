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
(ns org.apache.storm.scheduler.DefaultScheduler
  (:use [org.apache.storm util config])
  (:require [org.apache.storm.scheduler.EvenScheduler :as EvenScheduler])
  (:import [org.apache.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot SchedulerAssignment
            EvenScheduler ExecutorDetails])
  (:gen-class
    :implements [org.apache.storm.scheduler.IScheduler]))

(defn- bad-slots [existing-slots num-executors num-workers]
  (if (= 0 num-workers)
    '()
    (let [distribution (atom (integer-divided num-executors num-workers))
          keepers (atom {})]
      (doseq [[node+port executor-list] existing-slots :let [executor-count (count executor-list)]]
        (when (pos? (get @distribution executor-count 0))
          (swap! keepers assoc node+port executor-list)
          (swap! distribution update-in [executor-count] dec)
          ))
      (->> @keepers
           keys
           (apply dissoc existing-slots)
           keys
           (map (fn [[node port]]
                  (WorkerSlot. node port)))))))

(defn slots-can-reassign [^Cluster cluster slots]
  (->> slots
      (filter
        (fn [[node port]]
          (if-not (.isBlackListed cluster node)
            (if-let [supervisor (.getSupervisorById cluster node)]
              (.contains (.getAllPorts supervisor) (int port))
              ))))))

(defn -prepare [this conf]
  )

(defn default-schedule [^Topologies topologies ^Cluster cluster]
  (let [needs-scheduling-topologies (.needsSchedulingTopologies cluster topologies)]
    (doseq [^TopologyDetails topology needs-scheduling-topologies
            :let [topology-id (.getId topology)
                  available-slots (->> (.getAvailableSlots cluster)
                                       (map #(vector (.getNodeId %) (.getPort %))))
                  all-executors (->> topology
                                     .getExecutors
                                     (map #(vector (.getStartTask %) (.getEndTask %)))
                                     set)
                  alive-assigned (EvenScheduler/get-alive-assigned-node+port->executors cluster topology-id)
                  alive-executors (->> alive-assigned vals (apply concat) set)
                  can-reassign-slots (slots-can-reassign cluster (keys alive-assigned))
                  total-slots-to-use (min (.getNumWorkers topology)
                                          (+ (count can-reassign-slots) (count available-slots)))
                  bad-slots (if (or (> total-slots-to-use (count alive-assigned)) 
                                    (not= alive-executors all-executors))
                                (bad-slots alive-assigned (count all-executors) total-slots-to-use)
                                [])]]
      (.freeSlots cluster bad-slots)
      (EvenScheduler/schedule-topologies-evenly (Topologies. {topology-id topology}) cluster))))

(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (default-schedule topologies cluster))
