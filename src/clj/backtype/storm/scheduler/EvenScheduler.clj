(ns backtype.storm.scheduler.EvenScheduler
  (:use [backtype.storm util log config])
  (:require [clojure.set :as set])
  (:import [backtype.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot ExecutorDetails])
  (:gen-class
    :implements [backtype.storm.scheduler.IScheduler]))

; calculate the infinite sequence that balance the usage across all ids
; the way it's calculated is like watershed
(defn watershed-distribute [used-id-list available-id-list]
  (let [id->count (->> used-id-list frequencies (sort-by second))
        unused-id-list (clojure.set/difference (set available-id-list) (set used-id-list))
        all-ids (concat unused-id-list (keys id->count))
        counts (concat (repeat (count unused-id-list) 0) (vals id->count))
        diff-id->count (map - (rest counts) (butlast counts))
        repeat-all-ids (concat (repeat all-ids))
        to-fill-nodes (map #(take (+ 1 %) all-ids) (range (count all-ids)))
        watershed-nodes (mapcat repeat diff-id->count to-fill-nodes)]
       (concat (apply concat watershed-nodes) (apply concat repeat-all-ids))))

; assign slots based on available slots in the way the slots usage is balanced across each host
(defn assign-slots [available-slots used-slots slots-to-assign]
  (if (empty? available-slots)
    []
  (let [available-node-list (set (map first available-slots))
       ]
    (loop [assigned-slots []
           available-slots available-slots
           node-list-to-use (watershed-distribute (filter #(contains? available-node-list %) (map first used-slots)) available-node-list)
           slots-to-assign slots-to-assign]
      (if (or (= 0 slots-to-assign) (= 0 (count available-slots)))
        assigned-slots
        (let [node-to-use (first node-list-to-use)
              slot-to-use (some #(when (= node-to-use (first %)) %) available-slots)
             ]
          (if (nil? slot-to-use)
            (recur
              assigned-slots
              available-slots
              (rest node-list-to-use)
              slots-to-assign)
            (recur
              (conj assigned-slots slot-to-use )
              (remove #(= slot-to-use %) available-slots)
              (rest node-list-to-use)
              (dec slots-to-assign)))))))))



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
        available-slots (->> (.getAvailableSlots cluster)
                             (map #(vector (.getNodeId %) (.getPort %))))
        used-slots (->> (.getUsedSlots cluster)
                        (map #(vector (.getNodeId %) (.getPort %))))
        all-executors (->> topology
                          .getExecutors
                          (map #(vector (.getStartTask %) (.getEndTask %)))
                          set)
        alive-assigned (get-alive-assigned-node+port->executors cluster topology-id)
        total-slots-to-use (min (.getNumWorkers topology)
                                (+ (count available-slots) (count alive-assigned)))
        reassign-slots (assign-slots available-slots used-slots (- total-slots-to-use (count alive-assigned)))
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
