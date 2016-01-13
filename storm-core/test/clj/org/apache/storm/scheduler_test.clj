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
(ns org.apache.storm.scheduler-test
  (:use [clojure test])
  (:use [org.apache.storm config testing])
  (:use [org.apache.storm.scheduler EvenScheduler])
  (:require [org.apache.storm.daemon [nimbus :as nimbus]])
  (:import [org.apache.storm.generated StormTopology])
  (:import [org.apache.storm.scheduler Cluster SupervisorDetails WorkerSlot ExecutorDetails
            SchedulerAssignmentImpl Topologies TopologyDetails]))

(defn clojurify-executor->slot [executorToSlot]
  (into {} (for [[executor slot] executorToSlot]
             {[(.getStartTask executor) (.getEndTask executor)]
              [(.getNodeId slot) (.getPort slot)]})))

(defn clojurify-executor->comp [executorToComp]
  (into {} (for [[executor component] executorToComp]
             {[(.getStartTask executor) (.getEndTask executor)] component})))

(defn clojurify-component->executors [compToExecutor]
  (into {} (for [[component executors] compToExecutor
                 :let [new-executors (set (map #(vector (.getStartTask %) (.getEndTask %)) executors))]]
             {component new-executors})))

(deftest test-supervisor-details
  (let [executor->slot {(ExecutorDetails. (int 1) (int 5)) (WorkerSlot. "supervisor1" (int 1))
                        (ExecutorDetails. (int 6) (int 10)) (WorkerSlot. "supervisor2" (int 2))}
        topology-id "topology1"
        assignment (SchedulerAssignmentImpl. topology-id executor->slot)]
    ;; test assign
    (.assign assignment (WorkerSlot. "supervisor1" 1)
             (list (ExecutorDetails. (int 11) (int 15)) (ExecutorDetails. (int 16) (int 20))))
    (is (= {[1 5] ["supervisor1" 1]
            [6 10] ["supervisor2" 2]
            [11 15] ["supervisor1" 1]
            [16 20] ["supervisor1" 1]}
           (clojurify-executor->slot (.getExecutorToSlot assignment))))
    ;; test isSlotOccupied
    (is (= true (.isSlotOccupied assignment (WorkerSlot. "supervisor2" (int 2)))))
    (is (= true (.isSlotOccupied assignment (WorkerSlot. "supervisor1" (int 1)))))

    ;; test isExecutorAssigned
    (is (= true (.isExecutorAssigned assignment (ExecutorDetails. (int 1) (int 5)))))
    (is (= false (.isExecutorAssigned assignment (ExecutorDetails. (int 21) (int 25)))))

    ;; test unassignBySlot
    (.unassignBySlot assignment (WorkerSlot. "supervisor1" (int 1)))
    (is (= {[6 10] ["supervisor2" 2]}
           (clojurify-executor->slot (.getExecutorToSlot assignment))))

    ))

(deftest test-topologies
  (let [executor1 (ExecutorDetails. (int 1) (int 5))
        executor2 (ExecutorDetails. (int 6) (int 10))
        topology1 (TopologyDetails. "topology1" {TOPOLOGY-NAME "topology-name-1"} (StormTopology.) 1
                                   {executor1 "spout1"
                                    executor2 "bolt1"})
        ;; test topology.selectExecutorToComponent
        executor->comp (.selectExecutorToComponent topology1 (list executor1))
        _ (is (= (clojurify-executor->comp {executor1 "spout1"})
                 (clojurify-executor->comp executor->comp)))
        ;; test topologies.getById
        topology2 (TopologyDetails. "topology2" {TOPOLOGY-NAME "topology-name-2"} (StormTopology.) 1 {})
        topologies (Topologies. {"topology1" topology1 "topology2" topology2})
        _ (is (= "topology1" (->> "topology1"
                                  (.getById topologies)
                                  .getId)))
        ;; test topologies.getByName
        _ (is (= "topology2" (->> "topology-name-2"
                                  (.getByName topologies)
                                  .getId)))
        ]
    )
  )

(deftest test-cluster
  (let [supervisor1 (SupervisorDetails. "supervisor1" "192.168.0.1" (list ) (map int (list 1 3 5 7 9)))
        supervisor2 (SupervisorDetails. "supervisor2" "192.168.0.2" (list ) (map int (list 2 4 6 8 10)))
        executor1 (ExecutorDetails. (int 1) (int 5))
        executor2 (ExecutorDetails. (int 6) (int 10))
        executor3 (ExecutorDetails. (int 11) (int 15))
        executor11 (ExecutorDetails. (int 100) (int 105))
        executor12 (ExecutorDetails. (int 106) (int 110))
        executor21 (ExecutorDetails. (int 201) (int 205))
        executor22 (ExecutorDetails. (int 206) (int 210))
        ;; topology1 needs scheduling: executor3 is NOT assigned a slot.
        topology1 (TopologyDetails. "topology1" {TOPOLOGY-NAME "topology-name-1"}
                                    (StormTopology.)
                                    2
                                    {executor1 "spout1"
                                     executor2 "bolt1"
                                     executor3 "bolt2"})
        ;; topology2 is fully scheduled
        topology2 (TopologyDetails. "topology2" {TOPOLOGY-NAME "topology-name-2"}
                                    (StormTopology.)
                                    2
                                    {executor11 "spout11"
                                     executor12 "bolt12"})
        ;; topology3 needs scheduling, since the assignment is squeezed
        topology3 (TopologyDetails. "topology3" {TOPOLOGY-NAME "topology-name-3"}
                                    (StormTopology.)
                                    2
                                    {executor21 "spout21"
                                     executor22 "bolt22"})
        topologies (Topologies. {"topology1" topology1 "topology2" topology2 "topology3" topology3})
        executor->slot1 {executor1 (WorkerSlot. "supervisor1" (int 1))
                         executor2 (WorkerSlot. "supervisor2" (int 2))}
        executor->slot2 {executor11  (WorkerSlot. "supervisor1" (int 3))
                         executor12 (WorkerSlot. "supervisor2" (int 4))}
        executor->slot3 {executor21 (WorkerSlot. "supervisor1" (int 5))
                         executor22 (WorkerSlot. "supervisor1" (int 5))}
        assignment1 (SchedulerAssignmentImpl. "topology1" executor->slot1)
        assignment2 (SchedulerAssignmentImpl. "topology2" executor->slot2)
        assignment3 (SchedulerAssignmentImpl. "topology3" executor->slot3)
        cluster (Cluster. (nimbus/standalone-nimbus)
                          {"supervisor1" supervisor1 "supervisor2" supervisor2}
                          {"topology1" assignment1 "topology2" assignment2 "topology3" assignment3}
                  nil)]
    ;; test Cluster constructor
    (is (= #{"supervisor1" "supervisor2"}
           (->> cluster
                .getSupervisors
                keys
                set)))
    (is (= #{"topology1" "topology2" "topology3"}
           (->> cluster
                .getAssignments
                keys
                set)))

    ;; test Cluster.getUnassignedExecutors
    (is (= (set (list executor3))
           (-> cluster
               (.getUnassignedExecutors topology1)
               set)))
    (is (= true
           (empty? (-> cluster
                       (.getUnassignedExecutors topology2)))))
    ;; test Cluster.needsScheduling
    (is (= true (.needsScheduling cluster topology1)))
    (is (= false (.needsScheduling cluster topology2)))
    (is (= true (.needsScheduling cluster topology3)))
    ;; test Cluster.needsSchedulingTopologies
    (is (= #{"topology1" "topology3"}
           (->> (.needsSchedulingTopologies cluster topologies)
                (map (fn [topology] (.getId topology)))
                set)))

    ;; test Cluster.getNeedsSchedulingExecutorToComponents
    (is (= {executor3 "bolt2"}
           (.getNeedsSchedulingExecutorToComponents cluster topology1)))
    (is (= true
           (empty? (.getNeedsSchedulingExecutorToComponents cluster topology2))))
    (is (= true
           (empty? (.getNeedsSchedulingExecutorToComponents cluster topology3))))

    ;; test Cluster.getNeedsSchedulingComponentToExecutors
    (is (= {"bolt2" #{[(.getStartTask executor3) (.getEndTask executor3)]}}
           (clojurify-component->executors (.getNeedsSchedulingComponentToExecutors cluster topology1))))
    (is (= true
           (empty? (.getNeedsSchedulingComponentToExecutors cluster topology2))))
    (is (= true
           (empty? (.getNeedsSchedulingComponentToExecutors cluster topology3))))

    ;; test Cluster.getUsedPorts
    (is (= #{1 3 5} (set (.getUsedPorts cluster supervisor1))))
    (is (= #{2 4} (set (.getUsedPorts cluster supervisor2))))
    (is (= #{1 3 5} (set (.getUsedPorts cluster supervisor1))))
    
    ;; test Cluster.getAvailablePorts
    (is (= #{7 9} (set (.getAvailablePorts cluster supervisor1))))
    (is (= #{6 8 10} (set (.getAvailablePorts cluster supervisor2))))

    ;; test Cluster.getAvailableSlots
    (is (= #{["supervisor1" 7] ["supervisor1" 9]} (set (map (fn [slot] [(.getNodeId slot) (.getPort slot)]) (.getAvailableSlots cluster supervisor1)))))
    (is (= #{["supervisor2" 6] ["supervisor2" 8] ["supervisor2" 10]} (set (map (fn [slot] [(.getNodeId slot) (.getPort slot)]) (.getAvailableSlots cluster supervisor2)))))
    ;; test Cluster.getAvailableSlots
    (is (= #{["supervisor1" 7] ["supervisor1" 9] ["supervisor2" 6] ["supervisor2" 8] ["supervisor2" 10]}
           (set (map (fn [slot] [(.getNodeId slot) (.getPort slot)]) (.getAvailableSlots cluster)))))
    ;; test Cluster.getAssignedNumWorkers
    (is (= 2 (.getAssignedNumWorkers cluster topology1)))
    (is (= 2 (.getAssignedNumWorkers cluster topology2)))
    (is (= 1 (.getAssignedNumWorkers cluster topology3)))

    ;; test Cluster.isSlotOccupied
    (is (= true (.isSlotOccupied cluster (WorkerSlot. "supervisor1" (int 1)))))
    (is (= true (.isSlotOccupied cluster (WorkerSlot. "supervisor1" (int 3)))))
    (is (= true (.isSlotOccupied cluster (WorkerSlot. "supervisor1" (int 5)))))
    (is (= false (.isSlotOccupied cluster (WorkerSlot. "supervisor1" (int 7)))))
    (is (= false (.isSlotOccupied cluster (WorkerSlot. "supervisor1" (int 9)))))
    (is (= true (.isSlotOccupied cluster (WorkerSlot. "supervisor2" (int 2)))))
    (is (= true (.isSlotOccupied cluster (WorkerSlot. "supervisor2" (int 4)))))
    (is (= false (.isSlotOccupied cluster (WorkerSlot. "supervisor2" (int 6)))))
    (is (= false (.isSlotOccupied cluster (WorkerSlot. "supervisor2" (int 8)))))
    (is (= false (.isSlotOccupied cluster (WorkerSlot. "supervisor2" (int 10)))))
    ;; test Cluster.getAssignmentById
    (is (= assignment1 (.getAssignmentById cluster "topology1")))
    (is (= assignment2 (.getAssignmentById cluster "topology2")))
    (is (= assignment3 (.getAssignmentById cluster "topology3")))
    ;; test Cluster.getSupervisorById
    (is (= supervisor1 (.getSupervisorById cluster "supervisor1")))
    (is (= supervisor2 (.getSupervisorById cluster "supervisor2")))
    ;; test Cluster.getSupervisorsByHost
    (is (= #{supervisor1} (set (.getSupervisorsByHost cluster "192.168.0.1"))))
    (is (= #{supervisor2} (set (.getSupervisorsByHost cluster "192.168.0.2"))))

    ;; ==== the following tests will change the state of the cluster, so put it here at the end ====
    ;; test Cluster.assign
    (.assign cluster (WorkerSlot. "supervisor1" (int 7)) "topology1" (list executor3))
    (is (= false (.needsScheduling cluster topology1)))
    (is (= true (.isSlotOccupied cluster (WorkerSlot. "supervisor1" (int 7)))))

    ;; revert the change
    (.freeSlot cluster (WorkerSlot. "supervisor1" (int 7)))

    ;; test Cluster.assign: if a executor is already assigned, there will be an exception
    (let [has-exception (try
                          (.assign cluster (WorkerSlot. "supervisor1" (int 9)) "topology1" (list executor1))
                          false
                          (catch Exception e
                            true))]
      (is (= true has-exception)))

    ;; test Cluster.assign: if a slot is occupied, there will be an exception
    (let [has-exception (try
                          (.assign cluster (WorkerSlot. "supervisor2" (int 4)) "topology1" (list executor3))
                          false
                          (catch Exception e
                            true))]
      (is (= true has-exception)))

    ;; test Cluster.freeSlot
    (.freeSlot cluster (WorkerSlot. "supervisor1" (int 7)))
    (is (= false (.isSlotOccupied cluster (WorkerSlot. "supervisor1" (int 7)))))

    ;; test Cluster.freeSlots
    (is (= true (.isSlotOccupied cluster (WorkerSlot. "supervisor1" (int 1)))))
    (is (= true (.isSlotOccupied cluster (WorkerSlot. "supervisor1" (int 3)))))
    (is (= true (.isSlotOccupied cluster (WorkerSlot. "supervisor1" (int 5)))))
    (.freeSlots cluster (list (WorkerSlot. "supervisor1" (int 1))
                              (WorkerSlot. "supervisor1" (int 3))
                              (WorkerSlot. "supervisor1" (int 5))))
    (is (= false (.isSlotOccupied cluster (WorkerSlot. "supervisor1" (int 1)))))
    (is (= false (.isSlotOccupied cluster (WorkerSlot. "supervisor1" (int 3)))))
    (is (= false (.isSlotOccupied cluster (WorkerSlot. "supervisor1" (int 5)))))
    ))

(deftest test-sort-slots
  ;; test supervisor2 has more free slots
  (is (= '(["supervisor2" 6700] ["supervisor1" 6700]
           ["supervisor2" 6701] ["supervisor1" 6701]
           ["supervisor2" 6702])
         (sort-slots [["supervisor1" 6700] ["supervisor1" 6701]
                      ["supervisor2" 6700] ["supervisor2" 6701] ["supervisor2" 6702]
                      ])))
  ;; test supervisor3 has more free slots
  (is (= '(["supervisor3" 6700] ["supervisor2" 6700] ["supervisor1" 6700]
           ["supervisor3" 6703] ["supervisor2" 6701] ["supervisor1" 6701]
           ["supervisor3" 6702] ["supervisor2" 6702]
           ["supervisor3" 6701])
         (sort-slots [["supervisor1" 6700] ["supervisor1" 6701]
                      ["supervisor2" 6700] ["supervisor2" 6701] ["supervisor2" 6702]
                      ["supervisor3" 6700] ["supervisor3" 6703] ["supervisor3" 6702] ["supervisor3" 6701]
                      ])))
    )

