(ns backtype.storm.scheduler.EvenScheduler
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

(defn- mk-scheduler-assignment [topology-id task->node+port]
  (SchedulerAssignment. topology-id
                        (into {} (for [[task [node port]] task->node+port]
                                   {task (WorkerSlot. node port)}))))


(defn- schedule-topology [^TopologyDetails topology ^Cluster cluster keeper-slots-fn]
  (let [topology-id (.getId topology)
        available-slots (->> (.getAvailableSlots cluster)
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
        keep-assigned (if keeper-slots-fn
                        (keeper-slots-fn alive-assigned (count all-task-ids) total-slots-to-use)
                        alive-assigned
                        )
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
      (log-message "Available slots: " (pr-str available-slots))
      )
    (mk-scheduler-assignment topology-id (merge stay-assignment reassignment))))

(defn schedule-topologies-evenly [^Topologies topologies ^Cluster cluster keeper-slots-fn]
  (let [needs-scheduling-topologies (.needsSchedulingTopologies cluster topologies)]
    (doseq [^TopologyDetails topology needs-scheduling-topologies
            :let [topology-id (.getId topology)
                  new-assignment (schedule-topology topology cluster keeper-slots-fn)]]
      (.setAssignmentById cluster topology-id new-assignment))))
  
(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (schedule-topologies-evenly topologies cluster nil))
