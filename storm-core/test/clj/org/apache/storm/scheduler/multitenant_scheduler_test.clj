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
(ns org.apache.storm.scheduler.multitenant-scheduler-test
  (:use [clojure test])
  (:use [org.apache.storm config testing log])
  (:require [org.apache.storm.daemon [nimbus :as nimbus]])
  (:import [org.apache.storm.generated StormTopology])
  (:import [org.apache.storm.scheduler Cluster SupervisorDetails WorkerSlot ExecutorDetails
            SchedulerAssignmentImpl Topologies TopologyDetails])
  (:import [org.apache.storm.scheduler.multitenant Node NodePool FreePool DefaultPool
            IsolatedPool MultitenantScheduler]))

(defn gen-supervisors [count]
  (into {} (for [id (range count)
                :let [supervisor (SupervisorDetails. (str "super" id) (str "host" id) (list ) (map int (list 1 2 3 4)))]]
            {(.getId supervisor) supervisor})))

(defn to-top-map [topologies]
  (into {} (for [top topologies] {(.getId top) top})))

(defn ed [id] (ExecutorDetails. (int id) (int id)))

(defn mk-ed-map [arg]
  (into {}
    (for [[name start end] arg]
      (into {}
        (for [at (range start end)]
          {(ed at) name})))))

(deftest test-node
  (let [supers (gen-supervisors 5)
       cluster (Cluster. (nimbus/standalone-nimbus) supers {} nil)
       node-map (Node/getAllNodesFrom cluster)]
    (is (= 5 (.size node-map)))
    (let [node (.get node-map "super0")]
      (is (= "super0" (.getId node)))
      (is (= true (.isAlive node)))
      (is (= 0 (.size (.getRunningTopologies node))))
      (is (= true (.isTotallyFree node)))
      (is (= 4 (.totalSlotsFree node)))
      (is (= 0 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
      (.assign node "topology1" (list (ExecutorDetails. 1 1)) cluster)
      (is (= 1 (.size (.getRunningTopologies node))))
      (is (= false (.isTotallyFree node)))
      (is (= 3 (.totalSlotsFree node)))
      (is (= 1 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
      (.assign node "topology1" (list (ExecutorDetails. 2 2)) cluster)
      (is (= 1 (.size (.getRunningTopologies node))))
      (is (= false (.isTotallyFree node)))
      (is (= 2 (.totalSlotsFree node)))
      (is (= 2 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
      (.assign node "topology2" (list (ExecutorDetails. 1 1)) cluster)
      (is (= 2 (.size (.getRunningTopologies node))))
      (is (= false (.isTotallyFree node)))
      (is (= 1 (.totalSlotsFree node)))
      (is (= 3 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
      (.assign node "topology2" (list (ExecutorDetails. 2 2)) cluster)
      (is (= 2 (.size (.getRunningTopologies node))))
      (is (= false (.isTotallyFree node)))
      (is (= 0 (.totalSlotsFree node)))
      (is (= 4 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
      (.freeAllSlots node cluster)
      (is (= 0 (.size (.getRunningTopologies node))))
      (is (= true (.isTotallyFree node)))
      (is (= 4 (.totalSlotsFree node)))
      (is (= 0 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
    )))

(deftest test-free-pool
  (let [supers (gen-supervisors 5)
       cluster (Cluster. (nimbus/standalone-nimbus) supers {} nil)
       node-map (Node/getAllNodesFrom cluster)
       free-pool (FreePool. )]
    ;; assign one node so it is not in the pool
    (.assign (.get node-map "super0") "topology1" (list (ExecutorDetails. 1 1)) cluster)
    (.init free-pool cluster node-map)
    (is (= 4 (.nodesAvailable free-pool)))
    (is (= (* 4 4) (.slotsAvailable free-pool)))
    (let [ns-count-1 (.getNodeAndSlotCountIfSlotsWereTaken free-pool 1)
          ns-count-3 (.getNodeAndSlotCountIfSlotsWereTaken free-pool 3)
          ns-count-4 (.getNodeAndSlotCountIfSlotsWereTaken free-pool 4)
          ns-count-5 (.getNodeAndSlotCountIfSlotsWereTaken free-pool 5)]
      (is (= 1 (._nodes ns-count-1)))
      (is (= 4 (._slots ns-count-1)))
      (is (= 1 (._nodes ns-count-3)))
      (is (= 4 (._slots ns-count-3)))
      (is (= 1 (._nodes ns-count-4)))
      (is (= 4 (._slots ns-count-4)))
      (is (= 2 (._nodes ns-count-5)))
      (is (= 8 (._slots ns-count-5)))
    )
    (let [nodes (.takeNodesBySlots free-pool 5)]
      (is (= 2 (.size nodes)))
      (is (= 8 (Node/countFreeSlotsAlive nodes)))
      (is (= 8 (Node/countTotalSlotsAlive nodes)))
      (is (= 2 (.nodesAvailable free-pool)))
      (is (= (* 2 4) (.slotsAvailable free-pool)))
    )
    (let [nodes (.takeNodes free-pool 3)] ;;Only 2 should be left
      (is (= 2 (.size nodes)))
      (is (= 8 (Node/countFreeSlotsAlive nodes)))
      (is (= 8 (Node/countTotalSlotsAlive nodes)))
      (is (= 0 (.nodesAvailable free-pool)))
      (is (= 0 (.slotsAvailable free-pool)))
    )))

(deftest test-default-pool-simple
  (let [supers (gen-supervisors 5)
       cluster (Cluster. (nimbus/standalone-nimbus) supers {} nil)
       node-map (Node/getAllNodesFrom cluster)
       free-pool (FreePool. )
       default-pool (DefaultPool. )
       executor1 (ed 1)
       executor2 (ed 2)
       executor3 (ed 3)
       topology1 (TopologyDetails. "topology1"
                   {TOPOLOGY-NAME "topology-name-1"}
                   (StormTopology.)
                   2
                   {executor1 "spout1"
                    executor2 "bolt1"
                    executor3 "bolt2"})]
    ;; assign one node so it is not in the pool
    (.assign (.get node-map "super0") "topology1" (list executor1) cluster)
    (.init free-pool cluster node-map)
    (.init default-pool cluster node-map)
    (is (= true (.canAdd default-pool topology1)))
    (.addTopology default-pool topology1)
    ;;Only 1 node is in the default-pool because only one nodes was scheduled already
    (is (= 4 (.slotsAvailable default-pool)))
    (is (= 1 (.nodesAvailable default-pool)))
    (is (= (* 4 4) (.slotsAvailable free-pool)))
    (is (= 4 (.nodesAvailable free-pool)))
    (is (= 1 (.size (.getSlots (.getAssignmentById cluster "topology1")))))
    (.scheduleAsNeeded default-pool (into-array NodePool [free-pool]))
    (is (= 4 (.slotsAvailable default-pool)))
    (is (= 1 (.nodesAvailable default-pool)))
    (is (= (* 4 4) (.slotsAvailable free-pool)))
    (is (= 4 (.nodesAvailable free-pool)))
    (is (= 2 (.size (.getSlots (.getAssignmentById cluster "topology1")))))
    (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology1")))
))

(deftest test-default-pool-big-request
  (let [supers (gen-supervisors 5)
       cluster (Cluster. (nimbus/standalone-nimbus) supers {} nil)
       node-map (Node/getAllNodesFrom cluster)
       free-pool (FreePool. )
       default-pool (DefaultPool. )
       executor1 (ed 1)
       executor2 (ed 2)
       executor3 (ed 3)
       topology1 (TopologyDetails. "topology1"
                   {TOPOLOGY-NAME "topology-name-1"}
                   (StormTopology.)
                   5
                   {executor1 "spout1"
                    executor2 "bolt1"
                    executor3 "bolt2"})]
    ;; assign one node so it is not in the pool
    (.assign (.get node-map "super0") "topology1" (list executor1) cluster)
    (.init free-pool cluster node-map)
    (.init default-pool cluster node-map)
    (is (= true (.canAdd default-pool topology1)))
    (.addTopology default-pool topology1)
    ;;Only 1 node is in the default-pool because only one nodes was scheduled already
    (is (= 4 (.slotsAvailable default-pool)))
    (is (= 1 (.nodesAvailable default-pool)))
    (is (= (* 4 4) (.slotsAvailable free-pool)))
    (is (= 4 (.nodesAvailable free-pool)))
    (is (= 1 (.size (.getSlots (.getAssignmentById cluster "topology1")))))
    (.scheduleAsNeeded default-pool (into-array NodePool [free-pool]))
    (is (= 4 (.slotsAvailable default-pool)))
    (is (= 1 (.nodesAvailable default-pool)))
    (is (= (* 4 4) (.slotsAvailable free-pool)))
    (is (= 4 (.nodesAvailable free-pool)))
    (is (= 3 (.size (.getSlots (.getAssignmentById cluster "topology1")))))
    (is (= "Fully Scheduled (requested 5 slots, but could only use 3)" (.get (.getStatusMap cluster) "topology1")))
))

(deftest test-default-pool-big-request-2
  (let [supers (gen-supervisors 1)
       cluster (Cluster. (nimbus/standalone-nimbus) supers {} nil)
       node-map (Node/getAllNodesFrom cluster)
       free-pool (FreePool. )
       default-pool (DefaultPool. )
       executor1 (ed 1)
       executor2 (ed 2)
       executor3 (ed 3)
       executor4 (ed 4)
       executor5 (ed 5)
       topology1 (TopologyDetails. "topology1"
                   {TOPOLOGY-NAME "topology-name-1"}
                   (StormTopology.)
                   5
                   {executor1 "spout1"
                    executor2 "bolt1"
                    executor3 "bolt1"
                    executor4 "bolt1"
                    executor5 "bolt2"})]
    ;; assign one node so it is not in the pool
    (.assign (.get node-map "super0") "topology1" (list executor1) cluster)
    (.init free-pool cluster node-map)
    (.init default-pool cluster node-map)
    (is (= true (.canAdd default-pool topology1)))
    (.addTopology default-pool topology1)
    ;;Only 1 node is in the default-pool because only one nodes was scheduled already
    (is (= 4 (.slotsAvailable default-pool)))
    (is (= 1 (.nodesAvailable default-pool)))
    (is (= 0 (.slotsAvailable free-pool)))
    (is (= 0 (.nodesAvailable free-pool)))
    (is (= 1 (.size (.getSlots (.getAssignmentById cluster "topology1")))))
    (.scheduleAsNeeded default-pool (into-array NodePool [free-pool]))
    (is (= 4 (.slotsAvailable default-pool)))
    (is (= 1 (.nodesAvailable default-pool)))
    (is (= 0 (.slotsAvailable free-pool)))
    (is (= 0 (.nodesAvailable free-pool)))
    (is (= 4 (.size (.getSlots (.getAssignmentById cluster "topology1")))))
    (is (= "Running with fewer slots than requested (4/5)" (.get (.getStatusMap cluster) "topology1")))
))

(deftest test-default-pool-full
  (let [supers (gen-supervisors 2) ;;make 2 supervisors but only schedule with one of them
       single-super {(ffirst supers) (second (first supers))}
       single-cluster (Cluster. (nimbus/standalone-nimbus) single-super {} nil)
       executor1 (ed 1)
       executor2 (ed 2)
       executor3 (ed 3)
       executor4 (ed 4)
       executor5 (ed 5)
       topology1 (TopologyDetails. "topology1"
                   {TOPOLOGY-NAME "topology-name-1"}
                   (StormTopology.)
                   5
                   {executor1 "spout1"
                    executor2 "bolt1"
                    executor3 "bolt2"
                    executor4 "bolt3"
                    executor5 "bolt4"})]
    (let [node-map (Node/getAllNodesFrom single-cluster)
         free-pool (FreePool. )
         default-pool (DefaultPool. )]
      (.init free-pool single-cluster node-map)
      (.init default-pool single-cluster node-map)
      (.addTopology default-pool topology1)
      (.scheduleAsNeeded default-pool (into-array NodePool [free-pool]))
      ;; The cluster should be full and have 4 slots used, but the topology would like 1 more
      (is (= 4 (.size (.getUsedSlots single-cluster))))
      (is (= "Running with fewer slots than requested (4/5)" (.get (.getStatusMap single-cluster) "topology1")))
    )

    (let [cluster (Cluster. (nimbus/standalone-nimbus) supers (.getAssignments single-cluster) nil)
         node-map (Node/getAllNodesFrom cluster)
         free-pool (FreePool. )
         default-pool (DefaultPool. )]
      (.init free-pool cluster node-map)
      (.init default-pool cluster node-map)
      (.addTopology default-pool topology1)
      (.scheduleAsNeeded default-pool (into-array NodePool [free-pool]))
      ;; The cluster should now have 5 slots used
      (is (= 5 (.size (.getUsedSlots cluster))))
      (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology1")))
    )
))


(deftest test-default-pool-complex
  (let [supers (gen-supervisors 5)
       cluster (Cluster. (nimbus/standalone-nimbus) supers {} nil)
       node-map (Node/getAllNodesFrom cluster)
       free-pool (FreePool. )
       default-pool (DefaultPool. )
       executor1 (ed 1)
       executor2 (ed 2)
       executor3 (ed 3)
       executor11 (ed 11)
       executor12 (ed 12)
       executor13 (ed 13)
       executor14 (ed 14)
       topology1 (TopologyDetails. "topology1"
                   {TOPOLOGY-NAME "topology-name-1"}
                   (StormTopology.)
                   2
                   {executor1 "spout1"
                    executor2 "bolt1"
                    executor3 "bolt2"})
       topology2 (TopologyDetails. "topology2"
                    {TOPOLOGY-NAME "topology-name-2"}
                    (StormTopology.)
                    4
                    {executor11 "spout11"
                     executor12 "bolt12"
                     executor13 "bolt13"
                     executor14 "bolt14"})]
    ;; assign one node so it is not in the pool
    (.assign (.get node-map "super0") "topology1" (list executor1) cluster)
    (.init free-pool cluster node-map)
    (.init default-pool cluster node-map)
    (is (= true (.canAdd default-pool topology1)))
    (.addTopology default-pool topology1)
    (is (= true (.canAdd default-pool topology2)))
    (.addTopology default-pool topology2)
    ;;Only 1 node is in the default-pool because only one nodes was scheduled already
    (is (= 4 (.slotsAvailable default-pool)))
    (is (= 1 (.nodesAvailable default-pool)))
    (is (= (* 4 4) (.slotsAvailable free-pool)))
    (is (= 4 (.nodesAvailable free-pool)))
    (is (= 1 (.size (.getSlots (.getAssignmentById cluster "topology1")))))
    (is (= nil (.getAssignmentById cluster "topology2")))
    (.scheduleAsNeeded default-pool (into-array NodePool [free-pool]))
    ;;We steal a node from the free pool to handle the extra
    (is (= 8 (.slotsAvailable default-pool)))
    (is (= 2 (.nodesAvailable default-pool)))
    (is (= (* 3 4) (.slotsAvailable free-pool)))
    (is (= 3 (.nodesAvailable free-pool)))
    (is (= 2 (.size (.getSlots (.getAssignmentById cluster "topology1")))))
    (is (= 4 (.size (.getSlots (.getAssignmentById cluster "topology2")))))
    (let [ns-count-1 (.getNodeAndSlotCountIfSlotsWereTaken default-pool 1)
          ns-count-3 (.getNodeAndSlotCountIfSlotsWereTaken default-pool 3)
          ns-count-4 (.getNodeAndSlotCountIfSlotsWereTaken default-pool 4)
          ns-count-5 (.getNodeAndSlotCountIfSlotsWereTaken default-pool 5)]
      (is (= 1 (._nodes ns-count-1)))
      (is (= 4 (._slots ns-count-1)))
      (is (= 1 (._nodes ns-count-3)))
      (is (= 4 (._slots ns-count-3)))
      (is (= 1 (._nodes ns-count-4)))
      (is (= 4 (._slots ns-count-4)))
      (is (= 2 (._nodes ns-count-5)))
      (is (= 8 (._slots ns-count-5)))
    )
    (let [nodes (.takeNodesBySlots default-pool 3)]
      (is (= 1 (.size nodes)))
      (is (= 4 (Node/countFreeSlotsAlive nodes)))
      (is (= 4 (Node/countTotalSlotsAlive nodes)))
      (is (= 1 (.nodesAvailable default-pool)))
      (is (= (* 1 4) (.slotsAvailable default-pool)))
    )
    (let [nodes (.takeNodes default-pool 3)] ;;Only 1 should be left
      (is (= 1 (.size nodes)))
      (is (= 4 (Node/countFreeSlotsAlive nodes)))
      (is (= 4 (Node/countTotalSlotsAlive nodes)))
      (is (= 0 (.nodesAvailable default-pool)))
      (is (= 0 (.slotsAvailable default-pool)))
    )
    (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology1")))
    (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology2")))
))

(deftest test-isolated-pool-simple
  (let [supers (gen-supervisors 5)
       cluster (Cluster. (nimbus/standalone-nimbus) supers {} nil)
       node-map (Node/getAllNodesFrom cluster)
       free-pool (FreePool. )
       isolated-pool (IsolatedPool. 5)
       executor1 (ed 1)
       executor2 (ed 2)
       executor3 (ed 3)
       executor4 (ed 4)
       topology1 (TopologyDetails. "topology1"
                   {TOPOLOGY-NAME "topology-name-1"
                    TOPOLOGY-ISOLATED-MACHINES 4}
                   (StormTopology.)
                   4
                   {executor1 "spout1"
                    executor2 "bolt1"
                    executor3 "bolt2"
                    executor4 "bolt4"})]
    ;; assign one node so it is not in the pool
    (.assign (.get node-map "super0") "topology1" (list executor1) cluster)
    (.init free-pool cluster node-map)
    (.init isolated-pool cluster node-map)
    (is (= true (.canAdd isolated-pool topology1)))
    (.addTopology isolated-pool topology1)
    ;;Isolated topologies cannot have their resources stolen
    (is (= 0 (.slotsAvailable isolated-pool)))
    (is (= 0 (.nodesAvailable isolated-pool)))
    (is (= (* 4 4) (.slotsAvailable free-pool)))
    (is (= 4 (.nodesAvailable free-pool)))
    (is (= 1 (.size (.getSlots (.getAssignmentById cluster "topology1")))))
    (.scheduleAsNeeded isolated-pool (into-array NodePool [free-pool]))
    (is (= 0 (.slotsAvailable isolated-pool)))
    (is (= 0 (.nodesAvailable isolated-pool)))
    (is (= (* 1 4) (.slotsAvailable free-pool)))
    (is (= 1 (.nodesAvailable free-pool)))
    (let [assigned-slots (.getSlots (.getAssignmentById cluster "topology1"))]
      ;; 4 slots on 4 machines
      (is (= 4 (.size assigned-slots)))
      (is (= 4 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
    )
    (is (= "Scheduled Isolated on 4 Nodes" (.get (.getStatusMap cluster) "topology1")))
))

(deftest test-isolated-pool-big-ask
  (let [supers (gen-supervisors 5)
       cluster (Cluster. (nimbus/standalone-nimbus) supers {} nil)
       node-map (Node/getAllNodesFrom cluster)
       free-pool (FreePool. )
       isolated-pool (IsolatedPool. 5)
       executor1 (ed 1)
       executor2 (ed 2)
       executor3 (ed 3)
       executor4 (ed 4)
       topology1 (TopologyDetails. "topology1"
                   {TOPOLOGY-NAME "topology-name-1"
                    TOPOLOGY-ISOLATED-MACHINES 4}
                   (StormTopology.)
                   10
                   {executor1 "spout1"
                    executor2 "bolt1"
                    executor3 "bolt2"
                    executor4 "bolt4"})]
    ;; assign one node so it is not in the pool
    (.assign (.get node-map "super0") "topology1" (list executor1) cluster)
    (.init free-pool cluster node-map)
    (.init isolated-pool cluster node-map)
    (is (= true (.canAdd isolated-pool topology1)))
    (.addTopology isolated-pool topology1)
    ;;Isolated topologies cannot have their resources stolen
    (is (= 0 (.slotsAvailable isolated-pool)))
    (is (= 0 (.nodesAvailable isolated-pool)))
    (is (= (* 4 4) (.slotsAvailable free-pool)))
    (is (= 4 (.nodesAvailable free-pool)))
    (is (= 1 (.size (.getSlots (.getAssignmentById cluster "topology1")))))
    (.scheduleAsNeeded isolated-pool (into-array NodePool [free-pool]))
    (is (= 0 (.slotsAvailable isolated-pool)))
    (is (= 0 (.nodesAvailable isolated-pool)))
    (is (= (* 1 4) (.slotsAvailable free-pool)))
    (is (= 1 (.nodesAvailable free-pool)))
    (let [assigned-slots (.getSlots (.getAssignmentById cluster "topology1"))]
      ;; 4 slots on 4 machines
      (is (= 4 (.size assigned-slots)))
      (is (= 4 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
    )
    (is (= "Scheduled Isolated on 4 Nodes" (.get (.getStatusMap cluster) "topology1")))
))

(deftest test-isolated-pool-complex
  (let [supers (gen-supervisors 5)
       cluster (Cluster. (nimbus/standalone-nimbus) supers {} nil)
       node-map (Node/getAllNodesFrom cluster)
       free-pool (FreePool. )
       isolated-pool (IsolatedPool. 5)
       executor1 (ed 1)
       executor2 (ed 2)
       executor3 (ed 3)
       executor4 (ed 4)
       executor11 (ed 11)
       executor12 (ed 12)
       executor13 (ed 13)
       executor14 (ed 14)
       topology1 (TopologyDetails. "topology1"
                   {TOPOLOGY-NAME "topology-name-1"}
                   (StormTopology.)
                   4
                   {executor1 "spout1"
                    executor2 "bolt1"
                    executor3 "bolt2"
                    executor4 "bolt4"})
       topology2 (TopologyDetails. "topology2"
                    {TOPOLOGY-NAME "topology-name-2"
                     TOPOLOGY-ISOLATED-MACHINES 2}
                    (StormTopology.)
                    4
                    {executor11 "spout11"
                     executor12 "bolt12"
                     executor13 "bolt13"
                     executor14 "bolt14"})]
    ;; assign one node so it is not in the pool
    (.assign (.get node-map "super0") "topology1" (list executor1) cluster)
    (.init free-pool cluster node-map)
    (.init isolated-pool cluster node-map)
    (is (= true (.canAdd isolated-pool topology1)))
    (.addTopology isolated-pool topology1)
    (is (= true (.canAdd isolated-pool topology2)))
    (.addTopology isolated-pool topology2)
    ;; nodes can be stolen from non-isolted tops in the pool
    (is (= 4 (.slotsAvailable isolated-pool)))
    (is (= 1 (.nodesAvailable isolated-pool)))
    (is (= (* 4 4) (.slotsAvailable free-pool)))
    (is (= 4 (.nodesAvailable free-pool)))
    (is (= 1 (.size (.getSlots (.getAssignmentById cluster "topology1")))))
    (is (= nil (.getAssignmentById cluster "topology2")))
    (.scheduleAsNeeded isolated-pool (into-array NodePool [free-pool]))
    ;;We steal 2 nodes from the free pool to handle the extra (but still only 1 node for the non-isolated top
    (is (= 4 (.slotsAvailable isolated-pool)))
    (is (= 1 (.nodesAvailable isolated-pool)))
    (is (= (* 2 4) (.slotsAvailable free-pool)))
    (is (= 2 (.nodesAvailable free-pool)))
    (let [assigned-slots (.getSlots (.getAssignmentById cluster "topology1"))]
      ;; 4 slots on 1 machine
      (is (= 4 (.size assigned-slots)))
      (is (= 1 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
    )
    (let [assigned-slots (.getSlots (.getAssignmentById cluster "topology2"))]
      ;; 4 slots on 2 machines
      (is (= 4 (.size assigned-slots)))
      (is (= 2 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
    )

    (let [ns-count-1 (.getNodeAndSlotCountIfSlotsWereTaken isolated-pool 1)
          ns-count-3 (.getNodeAndSlotCountIfSlotsWereTaken isolated-pool 3)
          ns-count-4 (.getNodeAndSlotCountIfSlotsWereTaken isolated-pool 4)
          ns-count-5 (.getNodeAndSlotCountIfSlotsWereTaken isolated-pool 5)]
      (is (= 1 (._nodes ns-count-1)))
      (is (= 4 (._slots ns-count-1)))
      (is (= 1 (._nodes ns-count-3)))
      (is (= 4 (._slots ns-count-3)))
      (is (= 1 (._nodes ns-count-4)))
      (is (= 4 (._slots ns-count-4)))
      (is (= 1 (._nodes ns-count-5))) ;;Only 1 node can be stolen right now
      (is (= 4 (._slots ns-count-5)))
    )
    (let [nodes (.takeNodesBySlots isolated-pool 3)]
      (is (= 1 (.size nodes)))
      (is (= 4 (Node/countFreeSlotsAlive nodes)))
      (is (= 4 (Node/countTotalSlotsAlive nodes)))
      (is (= 0 (.nodesAvailable isolated-pool)))
      (is (= (* 0 4) (.slotsAvailable isolated-pool)))
    )
    (let [assigned-slots (.getSlots (.getAssignmentById cluster "topology1"))]
      ;; 4 slots on 1 machine
      (is (= 0 (.size assigned-slots)))
      (is (= 0 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
    )
    (let [assigned-slots (.getSlots (.getAssignmentById cluster "topology2"))]
      ;; 4 slots on 2 machines
      (is (= 4 (.size assigned-slots)))
      (is (= 2 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
    )
    (let [nodes (.takeNodes isolated-pool 3)] ;;Cannot steal from the isolated scheduler
      (is (= 0 (.size nodes)))
      (is (= 0 (Node/countFreeSlotsAlive nodes)))
      (is (= 0 (Node/countTotalSlotsAlive nodes)))
      (is (= 0 (.nodesAvailable isolated-pool)))
      (is (= 0 (.slotsAvailable isolated-pool)))
    )
    (is (= "Scheduled Isolated on 1 Nodes" (.get (.getStatusMap cluster) "topology1")))
    (is (= "Scheduled Isolated on 2 Nodes" (.get (.getStatusMap cluster) "topology2")))
))

(deftest test-isolated-pool-complex-2
  (let [supers (gen-supervisors 5)
       cluster (Cluster. (nimbus/standalone-nimbus) supers {} nil)
       node-map (Node/getAllNodesFrom cluster)
       free-pool (FreePool. )
       ;;like before but now we can only hold 2 nodes max.  Don't go over
       isolated-pool (IsolatedPool. 2)
       executor1 (ed 1)
       executor2 (ed 2)
       executor3 (ed 3)
       executor4 (ed 4)
       executor11 (ed 11)
       executor12 (ed 12)
       executor13 (ed 13)
       executor14 (ed 14)
       topology1 (TopologyDetails. "topology1"
                   {TOPOLOGY-NAME "topology-name-1"}
                   (StormTopology.)
                   4
                   {executor1 "spout1"
                    executor2 "bolt1"
                    executor3 "bolt2"
                    executor4 "bolt4"})
       topology2 (TopologyDetails. "topology2"
                    {TOPOLOGY-NAME "topology-name-2"
                     TOPOLOGY-ISOLATED-MACHINES 2}
                    (StormTopology.)
                    4
                    {executor11 "spout11"
                     executor12 "bolt12"
                     executor13 "bolt13"
                     executor14 "bolt14"})]
    ;; assign one node so it is not in the pool
    (.assign (.get node-map "super0") "topology1" (list executor1) cluster)
    (.init free-pool cluster node-map)
    (.init isolated-pool cluster node-map)
    (is (= true (.canAdd isolated-pool topology1)))
    (.addTopology isolated-pool topology1)
    (is (= true (.canAdd isolated-pool topology2)))
    (.addTopology isolated-pool topology2)
    ;; nodes can be stolen from non-isolted tops in the pool
    (is (= 4 (.slotsAvailable isolated-pool)))
    (is (= 1 (.nodesAvailable isolated-pool)))
    (is (= (* 4 4) (.slotsAvailable free-pool)))
    (is (= 4 (.nodesAvailable free-pool)))
    (is (= 1 (.size (.getSlots (.getAssignmentById cluster "topology1")))))
    (is (= nil (.getAssignmentById cluster "topology2")))
    (.scheduleAsNeeded isolated-pool (into-array NodePool [free-pool]))
    ;;We steal 1 node from the free pool and 1 from ourself to handle the extra
    (is (= 0 (.slotsAvailable isolated-pool)))
    (is (= 0 (.nodesAvailable isolated-pool)))
    (is (= (* 3 4) (.slotsAvailable free-pool)))
    (is (= 3 (.nodesAvailable free-pool)))
    (let [assigned-slots (.getSlots (.getAssignmentById cluster "topology1"))]
      ;; 0 slots on 0 machine
      (is (= 0 (.size assigned-slots)))
      (is (= 0 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
    )
    (let [assigned-slots (.getSlots (.getAssignmentById cluster "topology2"))]
      ;; 4 slots on 2 machines
      (is (= 4 (.size assigned-slots)))
      (is (= 2 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
    )
    ;;The text can be off for a bit until we schedule again
    (.scheduleAsNeeded isolated-pool (into-array NodePool [free-pool]))
    (is (= "Max Nodes(2) for this user would be exceeded. 1 more nodes needed to run topology." (.get (.getStatusMap cluster) "topology1")))
    (is (= "Scheduled Isolated on 2 Nodes" (.get (.getStatusMap cluster) "topology2")))
))

(deftest test-multitenant-scheduler
  (let [supers (gen-supervisors 10)
       topology1 (TopologyDetails. "topology1"
                   {TOPOLOGY-NAME "topology-name-1"
                    TOPOLOGY-SUBMITTER-USER "userC"}
                   (StormTopology.)
                   4
                   (mk-ed-map [["spout1" 0 5]
                               ["bolt1" 5 10]
                               ["bolt2" 10 15]
                               ["bolt3" 15 20]]))
       topology2 (TopologyDetails. "topology2"
                    {TOPOLOGY-NAME "topology-name-2"
                     TOPOLOGY-ISOLATED-MACHINES 2
                     TOPOLOGY-SUBMITTER-USER "userA"}
                    (StormTopology.)
                    4
                    (mk-ed-map [["spout11" 0 5]
                                ["bolt12" 5 6]
                                ["bolt13" 6 7]
                                ["bolt14" 7 10]]))
       topology3 (TopologyDetails. "topology3"
                    {TOPOLOGY-NAME "topology-name-3"
                     TOPOLOGY-ISOLATED-MACHINES 5
                     TOPOLOGY-SUBMITTER-USER "userB"}
                    (StormTopology.)
                    10
                    (mk-ed-map [["spout21" 0 10]
                                ["bolt22" 10 20]
                                ["bolt23" 20 30]
                                ["bolt24" 30 40]]))
       cluster (Cluster. (nimbus/standalone-nimbus) supers {} nil)
       node-map (Node/getAllNodesFrom cluster)
       topologies (Topologies. (to-top-map [topology1 topology2 topology3]))
       conf {MULTITENANT-SCHEDULER-USER-POOLS {"userA" 5 "userB" 5}}
       scheduler (MultitenantScheduler.)]
    (.assign (.get node-map "super0") "topology1" (list (ed 1)) cluster)
    (.assign (.get node-map "super1") "topology2" (list (ed 5)) cluster)
    (.prepare scheduler conf)
    (.schedule scheduler topologies cluster)
    (let [assignment (.getAssignmentById cluster "topology1")
          assigned-slots (.getSlots assignment)
          executors (.getExecutors assignment)]
      ;; 4 slots on 1 machine, all executors assigned
      (is (= 4 (.size assigned-slots)))
      (is (= 1 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
      (is (= 20 (.size executors)))
    )
    (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology1")))
    (is (= "Scheduled Isolated on 2 Nodes" (.get (.getStatusMap cluster) "topology2")))
    (is (= "Scheduled Isolated on 5 Nodes" (.get (.getStatusMap cluster) "topology3")))
))

(deftest test-force-free-slot-in-bad-state
  (let [supers (gen-supervisors 1)
        topology1 (TopologyDetails. "topology1"
                                    {TOPOLOGY-NAME "topology-name-1"
                                     TOPOLOGY-SUBMITTER-USER "userC"}
                                    (StormTopology.)
                                    4
                                    (mk-ed-map [["spout1" 0 5]
                                                ["bolt1" 5 10]
                                                ["bolt2" 10 15]
                                                ["bolt3" 15 20]]))
        existing-assignments {
                               "topology1" (SchedulerAssignmentImpl. "topology1" {(ExecutorDetails. 0 5) (WorkerSlot. "super0" 1)
                                                                                  (ExecutorDetails. 5 10) (WorkerSlot. "super0" 20)
                                                                                  (ExecutorDetails. 10 15) (WorkerSlot. "super0" 1)
                                                                                  (ExecutorDetails. 15 20) (WorkerSlot. "super0" 1)})
                               }
        cluster (Cluster. (nimbus/standalone-nimbus) supers existing-assignments nil)
        node-map (Node/getAllNodesFrom cluster)
        topologies (Topologies. (to-top-map [topology1]))
        conf {MULTITENANT-SCHEDULER-USER-POOLS {"userA" 5 "userB" 5}}
        scheduler (MultitenantScheduler.)]
    (.assign (.get node-map "super0") "topology1" (list (ed 1)) cluster)
    (.prepare scheduler conf)
    (.schedule scheduler topologies cluster)
    (let [assignment (.getAssignmentById cluster "topology1")
          assigned-slots (.getSlots assignment)
          executors (.getExecutors assignment)]
      (log-message "Executors are:" executors)
      ;; 4 slots on 1 machine, all executors assigned
      (is (= 4 (.size assigned-slots)))
      (is (= 1 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
      )
    (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology1")))
    ))

(deftest test-multitenant-scheduler-bad-starting-state
  (testing "Assiging same worker slot to different topologies is bad state"
    (let [supers (gen-supervisors 5)
          topology1 (TopologyDetails. "topology1"
                      {TOPOLOGY-NAME "topology-name-1"
                       TOPOLOGY-SUBMITTER-USER "userC"}
                      (StormTopology.)
                      1
                      (mk-ed-map [["spout1" 0 1]]))
          topology2 (TopologyDetails. "topology2"
                      {TOPOLOGY-NAME "topology-name-2"
                       TOPOLOGY-ISOLATED-MACHINES 2
                       TOPOLOGY-SUBMITTER-USER "userA"}
                      (StormTopology.)
                      2
                      (mk-ed-map [["spout11" 1 2]["bolt11" 3 4]]))
          topology3 (TopologyDetails. "topology3"
                      {TOPOLOGY-NAME "topology-name-3"
                       TOPOLOGY-ISOLATED-MACHINES 1
                       TOPOLOGY-SUBMITTER-USER "userB"}
                      (StormTopology.)
                      1
                      (mk-ed-map [["spout21" 2 3]]))
          worker-slot-with-multiple-assignments (WorkerSlot. "super1" 1)
          existing-assignments {"topology2" (SchedulerAssignmentImpl. "topology2" {(ExecutorDetails. 1 1) worker-slot-with-multiple-assignments})
                                "topology3" (SchedulerAssignmentImpl. "topology3" {(ExecutorDetails. 2 2) worker-slot-with-multiple-assignments})}
          cluster (Cluster. (nimbus/standalone-nimbus) supers existing-assignments nil)
          topologies (Topologies. (to-top-map [topology1 topology2 topology3]))
          conf {MULTITENANT-SCHEDULER-USER-POOLS {"userA" 2 "userB" 1}}
          scheduler (MultitenantScheduler.)]
      (.prepare scheduler conf)
      (.schedule scheduler topologies cluster)
      (let [assignment (.getAssignmentById cluster "topology1")
            assigned-slots (.getSlots assignment)
            executors (.getExecutors assignment)]
        (is (= 1 (.size assigned-slots))))
      (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology1")))
      (is (= "Scheduled Isolated on 2 Nodes" (.get (.getStatusMap cluster) "topology2")))
      (is (= "Scheduled Isolated on 1 Nodes" (.get (.getStatusMap cluster) "topology3"))))))

(deftest test-existing-assignment-slot-not-found-in-supervisor
  (testing "Scheduler should handle discrepancy when a live supervisor heartbeat does not report slot,
          but worker heartbeat says its running on that slot"
    (let [supers (gen-supervisors 1)
          port-not-reported-by-supervisor 6
          topology1 (TopologyDetails. "topology1"
                      {TOPOLOGY-NAME "topology-name-1"
                       TOPOLOGY-SUBMITTER-USER "userA"}
                      (StormTopology.)
                      1
                      (mk-ed-map [["spout11" 0 1]]))
          existing-assignments {"topology1"
                                (SchedulerAssignmentImpl. "topology1"
                                  {(ExecutorDetails. 0 0) (WorkerSlot. "super0" port-not-reported-by-supervisor)})}
          cluster (Cluster. (nimbus/standalone-nimbus) supers existing-assignments nil)
          topologies (Topologies. (to-top-map [topology1]))
          conf {}
          scheduler (MultitenantScheduler.)]
      (.prepare scheduler conf)
      (.schedule scheduler topologies cluster)
      (let [assignment (.getAssignmentById cluster "topology1")
            assigned-slots (.getSlots assignment)
            executors (.getExecutors assignment)]
        (is (= 1 (.size assigned-slots))))
      (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology1"))))))

(deftest test-existing-assignment-slot-on-dead-supervisor
  (testing "Dead supervisor could have slot with duplicate assignments or slot never reported by supervisor"
    (let [supers (gen-supervisors 1)
          dead-supervisor "super1"
          port-not-reported-by-supervisor 6
          topology1 (TopologyDetails. "topology1"
                      {TOPOLOGY-NAME "topology-name-1"
                       TOPOLOGY-SUBMITTER-USER "userA"}
                      (StormTopology.)
                      2
                      (mk-ed-map [["spout11" 0 1]
                                  ["bolt12" 1 2]]))
          topology2 (TopologyDetails. "topology2"
                      {TOPOLOGY-NAME "topology-name-2"
                       TOPOLOGY-SUBMITTER-USER "userA"}
                      (StormTopology.)
                      2
                      (mk-ed-map [["spout21" 4 5]
                                  ["bolt22" 5 6]]))
          worker-slot-with-multiple-assignments (WorkerSlot. dead-supervisor 1)
          existing-assignments {"topology1"
                                (SchedulerAssignmentImpl. "topology1"
                                  {(ExecutorDetails. 0 0) worker-slot-with-multiple-assignments
                                   (ExecutorDetails. 1 1) (WorkerSlot. dead-supervisor 3)})
                                "topology2"
                                (SchedulerAssignmentImpl. "topology2"
                                  {(ExecutorDetails. 4 4) worker-slot-with-multiple-assignments
                                   (ExecutorDetails. 5 5) (WorkerSlot. dead-supervisor port-not-reported-by-supervisor)})}
          cluster (Cluster. (nimbus/standalone-nimbus) supers existing-assignments nil)
          topologies (Topologies. (to-top-map [topology1 topology2]))
          conf {}
          scheduler (MultitenantScheduler.)]
      (.prepare scheduler conf)
      (.schedule scheduler topologies cluster)
      (let [assignment (.getAssignmentById cluster "topology1")
            assigned-slots (.getSlots assignment)
            executors (.getExecutors assignment)]
        (is (= 2 (.size assigned-slots)))
        (is (= 2 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
        (is (= 2 (.size executors))))
      (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology1")))
      (let [assignment (.getAssignmentById cluster "topology2")
            assigned-slots (.getSlots assignment)
            executors (.getExecutors assignment)]
        (is (= 2 (.size assigned-slots)))
        (is (= 2 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
        (is (= 2 (.size executors))))
      (is (= "Fully Scheduled" (.get (.getStatusMap cluster) "topology2"))))))


(deftest test-isolated-pool-scheduling-with-nodes-with-different-number-of-slots
  (let [super1 (SupervisorDetails. "super1" "host2" (list ) (map int (list 1 2 3 4 5)))
        super2 (SupervisorDetails. "super2" "host2" (list ) (map int (list 1 2 )))
        supers {"super1" super1 "super2" super2}
        topology1 (TopologyDetails. "topology1"
                    {TOPOLOGY-NAME "topology-name-1"
                     TOPOLOGY-SUBMITTER-USER "userA"
                     TOPOLOGY-ISOLATED-MACHINES 1}
                    (StormTopology.)
                    7
                    (mk-ed-map [["spout21" 0 7]]))
        existing-assignments {"topology1"
                              (SchedulerAssignmentImpl. "topology1"
                                {(ExecutorDetails. 0 0) (WorkerSlot. "super1" 1)
                                 (ExecutorDetails. 1 1) (WorkerSlot. "super1" 2)
                                 (ExecutorDetails. 2 2) (WorkerSlot. "super1" 3)
                                 (ExecutorDetails. 3 3) (WorkerSlot. "super1" 4)
                                 (ExecutorDetails. 4 4) (WorkerSlot. "super2" 1)
                                 (ExecutorDetails. 5 5) (WorkerSlot. "super2" 2)})}
        cluster (Cluster. (nimbus/standalone-nimbus) supers existing-assignments nil)
        topologies (Topologies. (to-top-map [topology1]))
        conf {MULTITENANT-SCHEDULER-USER-POOLS {"userA" 2}}
        scheduler (MultitenantScheduler.)]
    (.prepare scheduler conf)
    (.schedule scheduler topologies cluster)
    (is (= "Scheduled Isolated on 2 Nodes" (.get (.getStatusMap cluster) "topology1")))))
