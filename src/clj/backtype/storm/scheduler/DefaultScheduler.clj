(ns backtype.storm.scheduler.DefaultScheduler
  (:use [backtype.storm util])
  (:require [backtype.storm.scheduler.EvenScheduler :as EvenScheduler])
  (:import [backtype.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot SchedulerAssignment])
  (:gen-class
    :implements [backtype.storm.scheduler.IScheduler]))

(defn- keeper-slots [existing-slots num-executors num-workers]
  (if (= 0 num-workers)
    {}
    (let [distribution (atom (integer-divided num-executors num-workers))
          keepers (atom {})]
      (doseq [[node+port executor-list] existing-slots :let [executor-count (count executor-list)]]
        (when (pos? (get @distribution executor-count 0))
          (swap! keepers assoc node+port executor-list)
          (swap! distribution update-in [executor-count] dec)
          ))
      @keepers
      )))

   
(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (EvenScheduler/schedule-topologies-evenly topologies cluster keeper-slots))


