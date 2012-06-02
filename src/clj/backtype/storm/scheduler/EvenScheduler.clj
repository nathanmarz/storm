(ns backtype.storm.scheduler.EvenScheduler
  (:use [backtype.storm util log config])
  (:require [clojure.set :as set])
  (:import [backtype.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot SchedulerAssignment
            ExecutorDetails])
  (:gen-class
    :implements [backtype.storm.scheduler.IScheduler]))

(defn sort-slots [all-slots]
  (let [split-up (vals (group-by first all-slots))]
    (apply interleave-all split-up)
    ))

(defn- mk-scheduler-assignment [topology-id executor->node+port]
  (SchedulerAssignment. topology-id
                        (into {} (for [[executor [node port]] executor->node+port]
                                   {(ExecutorDetails. (first executor) (second executor)) (WorkerSlot. node port)}))))

(defn get-alive-assigned-node+port->executors [cluster topology-id]
  (let [existing-assignment (.getAssignmentById cluster topology-id)
        executor->slot (if existing-assignment
                         (.getExecutorToSlot existing-assignment)
                         {}) 
        executor->node+port (into {} (for [[^ExecutorDetails executor ^WorkerSlot slot] executor->slot
                                           :let [executor [(.getStartTask executor) (.getEndTask executor)]
                                                 node+port [(.getNodeId slot) (.getPort slot)]]]
                                       {executor node+port}))
        alive-assigned (reverse-map executor->node+port)]
    alive-assigned))

(defn- schedule-topology [^TopologyDetails topology ^Cluster cluster]
  (let [topology-id (.getId topology)
        topology-conf (.getConf topology)
        available-slots (->> (.getAvailableSlots cluster)
                             (map #(vector (.getNodeId %) (.getPort %))))
        all-executors (->> topology
                          .getExecutors
                          (map #(vector (.getStartTask %) (.getEndTask %)))
                          set)
        alive-assigned (get-alive-assigned-node+port->executors cluster topology-id)
        total-slots-to-use (min (topology-conf TOPOLOGY-WORKERS)
                                (+ (count available-slots) (count alive-assigned)))
        freed-slots (keys (apply dissoc alive-assigned (keys alive-assigned)))
        reassign-slots (take (- total-slots-to-use (count alive-assigned))
                             (sort-slots (concat available-slots freed-slots)))
        reassign-executors (sort (set/difference all-executors (set (apply concat (vals alive-assigned)))))
        reassignment (into {}
                           (map vector
                                reassign-executors
                                ;; for some reason it goes into infinite loop without limiting the repeat-seq
                                (repeat-seq (count reassign-executors) reassign-slots)))
        stay-assignment (into {} (mapcat (fn [[node+port executors]] (for [executor executors] [executor node+port])) alive-assigned))]
    (when-not (empty? reassignment)
      (log-message "Available slots: " (pr-str available-slots))
      )
    (mk-scheduler-assignment topology-id (merge stay-assignment reassignment))))

(defn schedule-topologies-evenly [^Topologies topologies ^Cluster cluster]
  (let [needs-scheduling-topologies (.needsSchedulingTopologies cluster topologies)]
    (doseq [^TopologyDetails topology needs-scheduling-topologies
            :let [topology-id (.getId topology)
                  new-assignment (schedule-topology topology cluster)]]
      (.setAssignmentById cluster topology-id new-assignment))))

(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (schedule-topologies-evenly topologies cluster))
