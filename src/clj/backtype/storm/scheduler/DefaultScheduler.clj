(ns backtype.storm.scheduler.DefaultScheduler
  (:use [backtype.storm util config])
  (:require [backtype.storm.scheduler.EvenScheduler :as EvenScheduler])
  (:import [backtype.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot SchedulerAssignment
            EvenScheduler ExecutorDetails])
  (:gen-class
    :implements [backtype.storm.scheduler.IScheduler]))

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
                  can-reassign-slots (slots-can-reassign cluster (keys alive-assigned))
                  total-slots-to-use (min (.getNumWorkers topology)
                                          (+ (count can-reassign-slots) (count available-slots)))
                  bad-slots (if (> total-slots-to-use (count alive-assigned))
                                (bad-slots alive-assigned (count all-executors) total-slots-to-use)
                                [])]]
      (.freeSlots cluster bad-slots)
      (EvenScheduler/schedule-topologies-evenly (Topologies. {topology-id topology}) cluster))))

(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (default-schedule topologies cluster))
