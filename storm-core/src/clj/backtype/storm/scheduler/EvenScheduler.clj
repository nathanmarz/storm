(ns backtype.storm.scheduler.EvenScheduler
  (:use [backtype.storm util log config])
  (:require [clojure.set :as set])
  (:import [backtype.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot ExecutorDetails])
  (:gen-class
    :implements [backtype.storm.scheduler.IScheduler]))

(defn sort-slots
  "used-slots, {supervisorid count(used-slots)}
   all-slots, ((supervisorid slot)......)"
  [all-slots used-slots]
  (loop [result [] all-slots (coll-to-map all-slots) used-slots used-slots]
    (let [all-slots (filter-val (complement empty?) all-slots)
          sorted-used (->> used-slots
                           (sort-by val)
                           (into {}))]    
      (if (empty? all-slots)
        result
        (let [slot (loop [all-slots all-slots sorted sorted-used]
                     (let [idle-supervisor (key (first sorted))
                           ports (get all-slots idle-supervisor nil)]
		                 (if ports
		                   [idle-supervisor (first ports)]
		                   (recur all-slots (dissoc sorted idle-supervisor)))))
		          sv (first slot)
		          inc-used (assoc sorted-used sv (inc (sorted-used sv)))
		          update-all (assoc all-slots sv (rest (all-slots sv)))]
          (recur (conj result slot) update-all inc-used))
        ))))

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

(defn get-supervisors-used-slots
  "return {supervisor-id count(used-slots)}"
  [cluster]
  (->> cluster
       (.getSupervisors)
       (map-val #(.getUsedPorts cluster %))
       (map-val count)
    )
  )

(defn- schedule-topology [^TopologyDetails topology ^Cluster cluster]
  (let [topology-id (.getId topology)
        available-slots (->> (.getAvailableSlots cluster)
                             (map #(vector (.getNodeId %) (.getPort %))))
        all-executors (->> topology
                          .getExecutors
                          (map #(vector (.getStartTask %) (.getEndTask %)))
                          set)
        alive-assigned (get-alive-assigned-node+port->executors cluster topology-id)
        total-slots-to-use (min (.getNumWorkers topology)
                                (+ (count available-slots) (count alive-assigned)))
        reassign-slots (take (- total-slots-to-use (count alive-assigned))
                             (sort-slots available-slots (get-supervisors-used-slots cluster)))
        reassign-executors (sort (set/difference all-executors (set (apply concat (vals alive-assigned)))))
        reassignment (into {}
                           (map vector
                                reassign-executors
                                ;; for some reason it goes into infinite loop without limiting the repeat-seq
                                (repeat-seq (count reassign-executors) reassign-slots)))]
    (when-not (empty? reassignment)
      (log-message "Available slots: " (pr-str available-slots))
      )
    reassignment))

(defn schedule-topologies-evenly [^Topologies topologies ^Cluster cluster]
  (let [needs-scheduling-topologies (.needsSchedulingTopologies cluster topologies)]
    (doseq [^TopologyDetails topology needs-scheduling-topologies
            :let [topology-id (.getId topology)
                  new-assignment (schedule-topology topology cluster)
                  node+port->executors (reverse-map new-assignment)]]
      (doseq [[node+port executors] node+port->executors
              :let [^WorkerSlot slot (WorkerSlot. (first node+port) (last node+port))
                    executors (for [[start-task end-task] executors]
                                (ExecutorDetails. start-task end-task))]]
        (.assign cluster slot topology-id executors)))))

(defn -prepare [this conf]
  )

(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (schedule-topologies-evenly topologies cluster))
