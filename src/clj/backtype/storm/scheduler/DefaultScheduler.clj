(ns backtype.storm.scheduler.DefaultScheduler
  (:use [backtype.storm util])
  (:require [backtype.storm.scheduler.EvenScheduler :as EvenScheduler])
  (:import [backtype.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot SchedulerAssignment])
  (:gen-class
    :implements [backtype.storm.scheduler.IScheduler]))

(defn- keeper-slots [existing-slots num-task-ids num-workers]
  (if (= 0 num-workers)
    {}
    (let [distribution (atom (integer-divided num-task-ids num-workers))
          keepers (atom {})]
      (doseq [[node+port task-list] existing-slots :let [task-count (count task-list)]]
        (when (pos? (get @distribution task-count 0))
          (swap! keepers assoc node+port task-list)
          (swap! distribution update-in [task-count] dec)
          ))
      @keepers
      )))

   
(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (EvenScheduler/schedule-topologies-evenly topologies cluster keeper-slots))


