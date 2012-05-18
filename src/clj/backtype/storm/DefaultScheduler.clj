(ns backtype.storm.DefaultScheduler
  (:use [backtype.storm util log config])
  (:require [clojure.set :as set])
  (:import [backtype.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot SchedulerAssignment])
  (:gen-class
    :implements [backtype.storm.scheduler.IScheduler]))

(defn sort-slots [all-slots]
  (let [split-up (vals (group-by first all-slots))]
    (apply interleave-all split-up)
    ))

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

(defn- mk-scheduler-assignment [topology-id task->node+port]
  (SchedulerAssignment. topology-id
                        (into {} (for [[task [node port]] task->node+port]
                                   {task (WorkerSlot. node port)}))))


(defn- schedule-topology [^TopologyDetails topology ^Cluster cluster]
  (let [topology-id (.getId topology)
        available-slots (->> topology-id
                             (.getAvailableSlots cluster)
                             (map #(vector (.getNodeId %) (.getPort %))))
        all-task-ids (set (.getTasks topology))
        existing-assignment (.getAssignmentById cluster topology-id)
        task->node+port (if-not existing-assignment
                          {}
                          (into {} (for [[task slot] (.getTaskToSlots existing-assignment)]
                                     {task [(.getNodeId slot) (.getPort slot)]})))
        alive-assigned (reverse-map task->node+port)
        topology-conf (.getConf topology)
        total-slots-to-use (min (topology-conf TOPOLOGY-WORKERS)
                                (+ (count available-slots) (count alive-assigned)))
        keep-assigned (keeper-slots alive-assigned (count all-task-ids) total-slots-to-use)
        freed-slots (keys (apply dissoc alive-assigned (keys keep-assigned)))
        reassign-slots (take (- total-slots-to-use (count keep-assigned))
                             (sort-slots (concat available-slots freed-slots)))
        reassign-ids (sort (set/difference all-task-ids (set (apply concat (vals keep-assigned)))))
        reassignment (into {}
                           (map vector
                                reassign-ids
                                ;; for some reason it goes into infinite loop without limiting the repeat-seq
                                (repeat-seq (count reassign-ids) reassign-slots)))
        stay-assignment (into {} (mapcat (fn [[node+port task-ids]] (for [id task-ids] [id node+port])) keep-assigned))]
    (when-not (empty? reassignment)
      (log-message "Reassigning " topology-id " to " total-slots-to-use " slots")
      (log-message "Reassign ids: " (vec reassign-ids))
      (log-message "Available slots: " (pr-str available-slots))
      )
    (mk-scheduler-assignment topology-id (merge stay-assignment reassignment))))
   
(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (let [needs-scheduling-topologies (.needsSchedulingTopologies cluster topologies)]
    (doseq [^TopologyDetails topology needs-scheduling-topologies
            :let [topology-id (.getId topology)
                  new-assignment (schedule-topology topology cluster)]]
      (.setAssignmentById cluster topology-id new-assignment))))


