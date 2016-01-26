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
(ns org.apache.storm.scheduler.resource-aware-scheduler-test
  (:use [clojure test])
  (:use [org.apache.storm config testing thrift])
  (:require [org.apache.storm.util :refer [map-val reverse-map sum]])
  (:require [org.apache.storm.daemon [nimbus :as nimbus]])
  (:import [org.apache.storm.generated StormTopology]
           [org.apache.storm Config]
           [org.apache.storm.testing TestWordSpout TestWordCounter]
           [org.apache.storm.topology TopologyBuilder])
  (:import [org.apache.storm.scheduler Cluster SupervisorDetails WorkerSlot ExecutorDetails
            SchedulerAssignmentImpl Topologies TopologyDetails])
  (:import [org.apache.storm.scheduler.resource RAS_Node RAS_Nodes ResourceAwareScheduler])
  (:import [org.apache.storm Config StormSubmitter])
  (:import [org.apache.storm LocalDRPC LocalCluster])
  (:import [java.util HashMap]))

(defn gen-supervisors [count ports]
  (into {} (for [id (range count)
                :let [supervisor (SupervisorDetails. (str "id" id)
                                       (str "host" id)
                                       (list ) (map int (range ports))
                                   {Config/SUPERVISOR_MEMORY_CAPACITY_MB 2000.0
                                    Config/SUPERVISOR_CPU_CAPACITY 400.0})]]
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
(def DEFAULT_PRIORITY_STRATEGY "org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy")
(def DEFAULT_EVICTION_STRATEGY "org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy")
(def DEFAULT_SCHEDULING_STRATEGY "org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy")

;; get the super->mem HashMap by counting the eds' mem usage of all topos on each super
(defn get-super->mem-usage [^Cluster cluster ^Topologies topologies]
  (let [assignments (.values (.getAssignments cluster))
        supers (.values (.getSupervisors cluster))
        super->mem-usage (HashMap.)
        _ (doseq [super supers] 
             (.put super->mem-usage super 0))]  ;; initialize the mem-usage as 0 for all supers
    (doseq [assignment assignments]
      (let [ed->super (into {}
                            (for [[ed slot] (.getExecutorToSlot assignment)]
                              {ed (.getSupervisorById cluster (.getNodeId slot))}))
            super->eds (reverse-map ed->super)
            topology (.getById topologies (.getTopologyId assignment))
            super->mem-pertopo (map-val (fn [eds] 
                                          (reduce + (map #(.getTotalMemReqTask topology %) eds))) 
                                        super->eds)]  ;; sum up the one topo's eds' mem usage on a super 
            (doseq [[super mem] super->mem-pertopo]
              (.put super->mem-usage 
                    super (+ mem (.get super->mem-usage super)))))) ;; add all topo's mem usage for each super
    super->mem-usage))

;; get the super->cpu HashMap by counting the eds' cpu usage of all topos on each super
(defn get-super->cpu-usage [^Cluster cluster ^Topologies topologies]
  (let [assignments (.values (.getAssignments cluster))
        supers (.values (.getSupervisors cluster))
        super->cpu-usage (HashMap.)
        _ (doseq [super supers] 
             (.put super->cpu-usage super 0))] ;; initialize the cpu-usage as 0 for all supers
    (doseq [assignment assignments]
      (let [ed->super (into {}
                            (for [[ed slot] (.getExecutorToSlot assignment)]
                              {ed (.getSupervisorById cluster (.getNodeId slot))}))
            super->eds (reverse-map ed->super)
            topology (.getById topologies (.getTopologyId assignment))
            super->cpu-pertopo (map-val (fn [eds] 
                                          (reduce + (map #(.getTotalCpuReqTask topology %) eds))) 
                                        super->eds)] ;; sum up the one topo's eds' cpu usage on a super 
            (doseq [[super cpu] super->cpu-pertopo]
              (.put super->cpu-usage 
                    super (+ cpu (.get super->cpu-usage super))))))  ;; add all topo's cpu usage for each super
    super->cpu-usage))

; testing resource/Node class
(deftest test-node
  (let [supers (gen-supervisors 5 4)
        cluster (Cluster. (nimbus/standalone-nimbus) supers {} {})
        topologies (Topologies. (to-top-map []))
        node-map (RAS_Nodes/getAllNodesFrom cluster topologies)
        topology1 (TopologyDetails. "topology1" {} nil 0)
        topology2 (TopologyDetails. "topology2" {} nil 0)]
    (is (= 5 (.size node-map)))
    (let [node (.get node-map "id0")]
      (is (= "id0" (.getId node)))
      (is (= true (.isAlive node)))
      (is (= 0 (.size (.getRunningTopologies node))))
      (is (= true (.isTotallyFree node)))
      (is (= 4 (.totalSlotsFree node)))
      (is (= 0 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
      (.assign node (.next (.iterator (.getFreeSlots node))) topology1 (list (ExecutorDetails. 1 1)))
      (is (= 1 (.size (.getRunningTopologies node))))
      (is (= false (.isTotallyFree node)))
      (is (= 3 (.totalSlotsFree node)))
      (is (= 1 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
      (.assign node (.next (.iterator (.getFreeSlots node))) topology1 (list (ExecutorDetails. 2 2)))
      (is (= 1 (.size (.getRunningTopologies node))))
      (is (= false (.isTotallyFree node)))
      (is (= 2 (.totalSlotsFree node)))
      (is (= 2 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
      (.assign node (.next (.iterator (.getFreeSlots node))) topology2 (list (ExecutorDetails. 1 1)))
      (is (= 2 (.size (.getRunningTopologies node))))
      (is (= false (.isTotallyFree node)))
      (is (= 1 (.totalSlotsFree node)))
      (is (= 3 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
      (.assign node (.next (.iterator (.getFreeSlots node))) topology2 (list (ExecutorDetails. 2 2)))
      (is (= 2 (.size (.getRunningTopologies node))))
      (is (= false (.isTotallyFree node)))
      (is (= 0 (.totalSlotsFree node)))
      (is (= 4 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
      (.freeAllSlots node)
      (is (= 0 (.size (.getRunningTopologies node))))
      (is (= true (.isTotallyFree node)))
      (is (= 4 (.totalSlotsFree node)))
      (is (= 0 (.totalSlotsUsed node)))
      (is (= 4 (.totalSlots node)))
    )))

(deftest test-sanity-resource-aware-scheduler
  (let [builder (TopologyBuilder.)
        _ (.setSpout builder "wordSpout" (TestWordSpout.) 1)
        _ (.shuffleGrouping (.setBolt builder "wordCountBolt" (TestWordCounter.) 1) "wordSpout")
        supers (gen-supervisors 1 2)
        storm-topology (.createTopology builder)
        topology1 (TopologyDetails. "topology1"
                    {TOPOLOGY-NAME "topology-name-1"
                     TOPOLOGY-SUBMITTER-USER "userC"
                     TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 128.0
                     TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                     TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT 10.0
                     TOPOLOGY-WORKER-MAX-HEAP-SIZE-MB 8192.0
                     TOPOLOGY-PRIORITY 0
                     TOPOLOGY-SCHEDULER-STRATEGY DEFAULT_SCHEDULING_STRATEGY}
                    storm-topology
                    1
                    (mk-ed-map [["wordSpout" 0 1]
                                ["wordCountBolt" 1 2]]))
        cluster (Cluster. (nimbus/standalone-nimbus) supers {}
                  {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                   "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping"})
        topologies (Topologies. (to-top-map [topology1]))
        node-map (RAS_Nodes/getAllNodesFrom cluster topologies)
        scheduler (ResourceAwareScheduler.)]
    (.prepare scheduler {RESOURCE-AWARE-SCHEDULER-EVICTION-STRATEGY DEFAULT_EVICTION_STRATEGY
                         RESOURCE-AWARE-SCHEDULER-PRIORITY-STRATEGY DEFAULT_PRIORITY_STRATEGY})
    (.schedule scheduler topologies cluster)
    (let [assignment (.getAssignmentById cluster "topology1")
          assigned-slots (.getSlots assignment)
          executors (.getExecutors assignment)]
      (is (= 1 (.size assigned-slots)))
      (is (= 1 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
      (is (= 2 (.size executors))))
    (is (= "Running - Fully Scheduled by DefaultResourceAwareStrategy" (.get (.getStatusMap cluster) "topology1")))))

(deftest test-topology-with-multiple-spouts
  (let [builder1 (TopologyBuilder.)  ;; a topology with multiple spouts
        _ (.setSpout builder1 "wordSpout1" (TestWordSpout.) 1)
        _ (.setSpout builder1 "wordSpout2" (TestWordSpout.) 1)
        _ (doto
            (.setBolt builder1 "wordCountBolt1" (TestWordCounter.) 1)
            (.shuffleGrouping "wordSpout1")
            (.shuffleGrouping "wordSpout2"))
        _ (.shuffleGrouping (.setBolt builder1 "wordCountBolt2" (TestWordCounter.) 1) "wordCountBolt1")
        _ (.shuffleGrouping (.setBolt builder1 "wordCountBolt3" (TestWordCounter.) 1) "wordCountBolt1")
        _ (.shuffleGrouping (.setBolt builder1 "wordCountBolt4" (TestWordCounter.) 1) "wordCountBolt2")
        _ (.shuffleGrouping (.setBolt builder1 "wordCountBolt5" (TestWordCounter.) 1) "wordSpout2")
        storm-topology1 (.createTopology builder1)
        topology1 (TopologyDetails. "topology1"
                    {TOPOLOGY-NAME "topology-name-1"
                     TOPOLOGY-SUBMITTER-USER "userC"
                     TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 128.0
                     TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                     TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT 10.0
                     TOPOLOGY-WORKER-MAX-HEAP-SIZE-MB 8192.0
                     TOPOLOGY-PRIORITY 0
                     TOPOLOGY-SCHEDULER-STRATEGY DEFAULT_SCHEDULING_STRATEGY}
                    storm-topology1
                    1
                    (mk-ed-map [["wordSpout1" 0 1]
                                ["wordSpout2" 1 2]
                                ["wordCountBolt1" 2 3]
                                ["wordCountBolt2" 3 4]
                                ["wordCountBolt3" 4 5]
                                ["wordCountBolt4" 5 6]
                                ["wordCountBolt5" 6 7]]))
        builder2 (TopologyBuilder.)  ;; a topology with two unconnected partitions
        _ (.setSpout builder2 "wordSpoutX" (TestWordSpout.) 1)
        _ (.setSpout builder2 "wordSpoutY" (TestWordSpout.) 1)
        storm-topology2 (.createTopology builder1)
        topology2 (TopologyDetails. "topology2"
                    {TOPOLOGY-NAME "topology-name-2"
                     TOPOLOGY-SUBMITTER-USER "userC"
                     TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 128.0
                     TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                     TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT 10.0
                     TOPOLOGY-WORKER-MAX-HEAP-SIZE-MB 8192.0
                     TOPOLOGY-PRIORITY 0
                     TOPOLOGY-SCHEDULER-STRATEGY DEFAULT_SCHEDULING_STRATEGY}
                    storm-topology2
                    1
                    (mk-ed-map [["wordSpoutX" 0 1]
                                ["wordSpoutY" 1 2]]))
        supers (gen-supervisors 2 4)
        cluster (Cluster. (nimbus/standalone-nimbus) supers {}
                  {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                   "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping"})
        topologies (Topologies. (to-top-map [topology1 topology2]))
        scheduler (ResourceAwareScheduler.)]
    (.prepare scheduler {RESOURCE-AWARE-SCHEDULER-EVICTION-STRATEGY DEFAULT_EVICTION_STRATEGY
                         RESOURCE-AWARE-SCHEDULER-PRIORITY-STRATEGY DEFAULT_PRIORITY_STRATEGY})
    (.schedule scheduler topologies cluster)
    (let [assignment (.getAssignmentById cluster "topology1")
          assigned-slots (.getSlots assignment)
          executors (.getExecutors assignment)]
      (is (= 1 (.size assigned-slots)))
      (is (= 1 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
      (is (= 7 (.size executors))))
    (is (= "Running - Fully Scheduled by DefaultResourceAwareStrategy" (.get (.getStatusMap cluster) "topology1")))
    (let [assignment (.getAssignmentById cluster "topology2")
          assigned-slots (.getSlots assignment)
          executors (.getExecutors assignment)]
      (is (= 1 (.size assigned-slots)))
      (is (= 1 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
      (is (= 2 (.size executors))))
    (is (= "Running - Fully Scheduled by DefaultResourceAwareStrategy" (.get (.getStatusMap cluster) "topology2")))))

(deftest test-topology-set-memory-and-cpu-load
  (let [builder (TopologyBuilder.)
        _ (.setSpout builder "wordSpout" (TestWordSpout.) 1)
        _ (doto
            (.setBolt builder "wordCountBolt" (TestWordCounter.) 1)
            (.setMemoryLoad 110.0)
            (.setCPULoad 20.0)
            (.shuffleGrouping "wordSpout"))
        supers (gen-supervisors 2 2)  ;; to test whether two tasks will be assigned to one or two nodes
        storm-topology (.createTopology builder)
        topology2 (TopologyDetails. "topology2"
                    {TOPOLOGY-NAME "topology-name-2"
                     TOPOLOGY-SUBMITTER-USER "userC"
                     TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 128.0
                     TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                     TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT 10.0
                     TOPOLOGY-WORKER-MAX-HEAP-SIZE-MB 8192.0
                     TOPOLOGY-PRIORITY 0
                     TOPOLOGY-SCHEDULER-STRATEGY DEFAULT_SCHEDULING_STRATEGY}
                    storm-topology
                    2
                    (mk-ed-map [["wordSpout" 0 1]
                                ["wordCountBolt" 1 2]]))
        cluster (Cluster. (nimbus/standalone-nimbus) supers {}
                  {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                   "org.apache.storm.testing.AlternateRackDNSToSwitchMapping"})
        topologies (Topologies. (to-top-map [topology2]))
        scheduler (ResourceAwareScheduler.)]
    (.prepare scheduler {RESOURCE-AWARE-SCHEDULER-EVICTION-STRATEGY DEFAULT_EVICTION_STRATEGY
                         RESOURCE-AWARE-SCHEDULER-PRIORITY-STRATEGY DEFAULT_PRIORITY_STRATEGY})
    (.schedule scheduler topologies cluster)
    (let [assignment (.getAssignmentById cluster "topology2")
          assigned-slots (.getSlots assignment)
          executors (.getExecutors assignment)]
      ;; 4 slots on 1 machine, all executors assigned
      (is (= 1 (.size assigned-slots)))
      (is (= 1 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
      (is (= 2 (.size executors))))
    (is (= "Running - Fully Scheduled by DefaultResourceAwareStrategy" (.get (.getStatusMap cluster) "topology2")))))

(deftest test-resource-limitation
  (let [builder (TopologyBuilder.)
        _ (doto (.setSpout builder "wordSpout" (TestWordSpout.) 2)
            (.setMemoryLoad 1000.0 200.0)
            (.setCPULoad 250.0))
        _ (doto (.setBolt builder "wordCountBolt" (TestWordCounter.) 1)
            (.shuffleGrouping  "wordSpout")
            (.setMemoryLoad 500.0 100.0)
            (.setCPULoad 100.0))
        supers (gen-supervisors 2 2)  ;; need at least two nodes to hold these executors
        storm-topology (.createTopology builder)
        topology1 (TopologyDetails. "topology1"
                    {TOPOLOGY-NAME "topology-name-1"
                     TOPOLOGY-SUBMITTER-USER "userC"
                     TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 128.0
                     TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                     TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT 10.0
                     TOPOLOGY-WORKER-MAX-HEAP-SIZE-MB 8192.0
                     TOPOLOGY-PRIORITY 0
                     TOPOLOGY-SCHEDULER-STRATEGY DEFAULT_SCHEDULING_STRATEGY}
                    storm-topology
                    2 ;; need two workers, each on one node
                    (mk-ed-map [["wordSpout" 0 2]
                                ["wordCountBolt" 2 3]]))
        cluster (Cluster. (nimbus/standalone-nimbus) supers {}
                  {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                   "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping"})
        topologies (Topologies. (to-top-map [topology1]))
        scheduler (ResourceAwareScheduler.)]
    (.prepare scheduler {RESOURCE-AWARE-SCHEDULER-EVICTION-STRATEGY DEFAULT_EVICTION_STRATEGY
                         RESOURCE-AWARE-SCHEDULER-PRIORITY-STRATEGY DEFAULT_PRIORITY_STRATEGY})
    (.schedule scheduler topologies cluster)
    (let [assignment (.getAssignmentById cluster "topology1")
          assigned-slots (.getSlots assignment)
          node-ids (map #(.getNodeId %) assigned-slots)
          executors (.getExecutors assignment)
          epsilon 0.000001
          assigned-ed-mem (sort (map #(.getTotalMemReqTask topology1 %) executors))
          assigned-ed-cpu (sort (map #(.getTotalCpuReqTask topology1 %) executors))
          ed->super (into {}
                            (for [[ed slot] (.getExecutorToSlot assignment)]
                              {ed (.getSupervisorById cluster (.getNodeId slot))}))
          super->eds (reverse-map ed->super)
          mem-avail->used (into []
                                 (for [[super eds] super->eds]
                                   [(.getTotalMemory super) (sum (map #(.getTotalMemReqTask topology1 %) eds))]))
          cpu-avail->used (into []
                                 (for [[super eds] super->eds]
                                   [(.getTotalCPU super) (sum (map #(.getTotalCpuReqTask topology1 %) eds))]))]
    ;; 4 slots on 1 machine, all executors assigned
    (is (= 2 (.size assigned-slots)))  ;; executor0 resides one one worker (on one), executor1 and executor2 on another worker (on the other node)
    (is (= 2 (.size (into #{} (for [slot assigned-slots] (.getNodeId slot))))))
    (is (= 3 (.size executors)))
    ;; make sure resource (mem/cpu) assigned equals to resource specified
    (is (< (Math/abs (- 600.0 (first assigned-ed-mem))) epsilon))
    (is (< (Math/abs (- 1200.0 (second assigned-ed-mem))) epsilon))
    (is (< (Math/abs (- 1200.0 (last assigned-ed-mem))) epsilon))
    (is (< (Math/abs (- 100.0 (first assigned-ed-cpu))) epsilon))
    (is (< (Math/abs (- 250.0 (second assigned-ed-cpu))) epsilon))
    (is (< (Math/abs (- 250.0 (last assigned-ed-cpu))) epsilon))
    (doseq [[avail used] mem-avail->used] ;; for each node, assigned mem smaller than total
      (is (>= avail used)))
    (doseq [[avail used] cpu-avail->used] ;; for each node, assigned cpu smaller than total
      (is (>= avail used))))
  (is (= "Running - Fully Scheduled by DefaultResourceAwareStrategy" (.get (.getStatusMap cluster) "topology1")))))

(deftest test-scheduling-resilience
  (let [supers (gen-supervisors 2 2)
         builder1 (TopologyBuilder.)
         _ (.setSpout builder1 "spout1" (TestWordSpout.) 2)
         storm-topology1 (.createTopology builder1)
         topology1 (TopologyDetails. "topology1"
                     {TOPOLOGY-NAME "topology-name-1"
                      TOPOLOGY-SUBMITTER-USER "userC"
                      TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 128.0
                      TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                      TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT 10.0
                      TOPOLOGY-WORKER-MAX-HEAP-SIZE-MB 8192.0
                      TOPOLOGY-PRIORITY 0
                      TOPOLOGY-SCHEDULER-STRATEGY DEFAULT_SCHEDULING_STRATEGY}
                     storm-topology1
                     3 ;; three workers to hold three executors
                     (mk-ed-map [["spout1" 0 3]]))
         builder2 (TopologyBuilder.)
         _ (.setSpout builder2 "spout2" (TestWordSpout.) 2)
         storm-topology2 (.createTopology builder2)
         topology2 (TopologyDetails. "topology2"
                     {TOPOLOGY-NAME "topology-name-2"
                      TOPOLOGY-SUBMITTER-USER "userC"
                      TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 1280.0 ;; large enough thus two eds can not be fully assigned to one node
                      TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                      TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT 10.0
                      TOPOLOGY-WORKER-MAX-HEAP-SIZE-MB 8192.0
                      TOPOLOGY-PRIORITY 0
                      TOPOLOGY-SCHEDULER-STRATEGY DEFAULT_SCHEDULING_STRATEGY}
                     storm-topology2
                     2  ;; two workers, each holds one executor and resides on one node
                     (mk-ed-map [["spout2" 0 2]]))
        scheduler (ResourceAwareScheduler.)]

    (testing "When a worker fails, RAS does not alter existing assignments on healthy workers"
      (let [cluster (Cluster. (nimbus/standalone-nimbus) supers {}
                              {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                               "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping"})
            topologies (Topologies. (to-top-map [topology2]))
            _ (.prepare scheduler {RESOURCE-AWARE-SCHEDULER-EVICTION-STRATEGY DEFAULT_EVICTION_STRATEGY
                                   RESOURCE-AWARE-SCHEDULER-PRIORITY-STRATEGY DEFAULT_PRIORITY_STRATEGY})
            _ (.schedule scheduler topologies cluster)
            assignment (.getAssignmentById cluster "topology2")
            failed-worker (first (vec (.getSlots assignment)))  ;; choose a worker to mock as failed
            ed->slot (.getExecutorToSlot assignment)
            failed-eds (.get (reverse-map ed->slot) failed-worker)
            _ (doseq [ed failed-eds] (.remove ed->slot ed))  ;; remove executor details assigned to the worker
            copy-old-mapping (HashMap. ed->slot)
            healthy-eds (.keySet copy-old-mapping)
            _ (.prepare scheduler {RESOURCE-AWARE-SCHEDULER-EVICTION-STRATEGY DEFAULT_EVICTION_STRATEGY
                                   RESOURCE-AWARE-SCHEDULER-PRIORITY-STRATEGY DEFAULT_PRIORITY_STRATEGY})
            _ (.schedule scheduler topologies cluster)
            new-assignment (.getAssignmentById cluster "topology2")
            new-ed->slot (.getExecutorToSlot new-assignment)]
        ;; for each executor that was scheduled on healthy workers, their slots should remain unchanged after a new scheduling
        (doseq [ed healthy-eds]
          (is (.equals (.get copy-old-mapping ed) (.get new-ed->slot ed))))
        (is (= "Running - Fully Scheduled by DefaultResourceAwareStrategy" (.get (.getStatusMap cluster) "topology2")))))

    (testing "When a supervisor fails, RAS does not alter existing assignments"
      (let [existing-assignments {"topology1" (SchedulerAssignmentImpl. "topology1"
                                                                        {(ExecutorDetails. 0 0) (WorkerSlot. "id0" 0)    ;; worker 0 on the failed super
                                                                         (ExecutorDetails. 1 1) (WorkerSlot. "id0" 1)    ;; worker 1 on the failed super
                                                                         (ExecutorDetails. 2 2) (WorkerSlot. "id1" 1)})} ;; worker 2 on the health super
            cluster (Cluster. (nimbus/standalone-nimbus) supers existing-assignments
                              {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                               "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping"})
            topologies (Topologies. (to-top-map [topology1]))
            assignment (.getAssignmentById cluster "topology1")
            ed->slot (.getExecutorToSlot assignment)
            copy-old-mapping (HashMap. ed->slot)
            existing-eds (.keySet copy-old-mapping)  ;; all the three eds on three workers
            new-cluster (Cluster. (nimbus/standalone-nimbus)
                                  (dissoc supers "id0")        ;; mock the super0 as a failed supervisor
                                  (.getAssignments cluster)
                                  {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                                   "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping"})
            _ (.prepare scheduler {RESOURCE-AWARE-SCHEDULER-EVICTION-STRATEGY DEFAULT_EVICTION_STRATEGY
                                   RESOURCE-AWARE-SCHEDULER-PRIORITY-STRATEGY DEFAULT_PRIORITY_STRATEGY})
            _ (.schedule scheduler topologies new-cluster) ;; the actual schedule for this topo will not run since it is fully assigned
            new-assignment (.getAssignmentById new-cluster "topology1")
            new-ed->slot (.getExecutorToSlot new-assignment)]
        (doseq [ed existing-eds]
          (is (.equals (.get copy-old-mapping ed) (.get new-ed->slot ed))))
        (is (= "Fully Scheduled" (.get (.getStatusMap new-cluster) "topology1")))))

    (testing "When a supervisor and a worker on it fails, RAS does not alter existing assignments"
      (let [existing-assignments {"topology1" (SchedulerAssignmentImpl. "topology1"
                                                                        {(ExecutorDetails. 0 0) (WorkerSlot. "id0" 1)    ;; the worker to orphan
                                                                         (ExecutorDetails. 1 1) (WorkerSlot. "id0" 2)    ;; the worker to kill
                                                                         (ExecutorDetails. 2 2) (WorkerSlot. "id1" 1)})} ;; the healthy worker
            cluster (Cluster. (nimbus/standalone-nimbus) supers existing-assignments
                              {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                               "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping"})
            topologies (Topologies. (to-top-map [topology1]))
            assignment (.getAssignmentById cluster "topology1")
            ed->slot (.getExecutorToSlot assignment)
            _ (.remove ed->slot (ExecutorDetails. 1 1))  ;; delete one worker of super0 (failed) from topo1 assignment to enable actual schedule for testing
            copy-old-mapping (HashMap. ed->slot)
            existing-eds (.keySet copy-old-mapping)  ;; namely the two eds on the orphaned worker and the healthy worker
            new-cluster (Cluster. (nimbus/standalone-nimbus)
                                  (dissoc supers "id0")        ;; mock the super0 as a failed supervisor
                                  (.getAssignments cluster)
                                  {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                                   "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping"})
            _ (.prepare scheduler {RESOURCE-AWARE-SCHEDULER-EVICTION-STRATEGY DEFAULT_EVICTION_STRATEGY
                                   RESOURCE-AWARE-SCHEDULER-PRIORITY-STRATEGY DEFAULT_PRIORITY_STRATEGY})
            _ (.schedule scheduler topologies new-cluster)
            new-assignment (.getAssignmentById new-cluster "topology1")
            new-ed->slot (.getExecutorToSlot new-assignment)]
        (doseq [ed existing-eds]
          (is (.equals (.get copy-old-mapping ed) (.get new-ed->slot ed))))
        (is (= "Running - Fully Scheduled by DefaultResourceAwareStrategy" (.get (.getStatusMap new-cluster) "topology1")))))

    (testing "Scheduling a new topology does not disturb other assignments unnecessarily"
      (let [cluster (Cluster. (nimbus/standalone-nimbus) supers {}
                              {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                               "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping"})
            topologies (Topologies. (to-top-map [topology1]))
            _ (.prepare scheduler {RESOURCE-AWARE-SCHEDULER-EVICTION-STRATEGY DEFAULT_EVICTION_STRATEGY
                                   RESOURCE-AWARE-SCHEDULER-PRIORITY-STRATEGY DEFAULT_PRIORITY_STRATEGY})
            _ (.schedule scheduler topologies cluster)
            assignment (.getAssignmentById cluster "topology1")
            ed->slot (.getExecutorToSlot assignment)
            copy-old-mapping (HashMap. ed->slot)
            new-topologies (Topologies. (to-top-map [topology1 topology2]))  ;; a second topology joins
            _ (.prepare scheduler {RESOURCE-AWARE-SCHEDULER-EVICTION-STRATEGY DEFAULT_EVICTION_STRATEGY
                                   RESOURCE-AWARE-SCHEDULER-PRIORITY-STRATEGY DEFAULT_PRIORITY_STRATEGY})
            _ (.schedule scheduler new-topologies cluster)
            new-assignment (.getAssignmentById cluster "topology1")
            new-ed->slot (.getExecutorToSlot new-assignment)]
        (doseq [ed (.keySet copy-old-mapping)]
          (is (.equals (.get copy-old-mapping ed) (.get new-ed->slot ed))))  ;; the assignment for topo1 should not change
        (is (= "Running - Fully Scheduled by DefaultResourceAwareStrategy" (.get (.getStatusMap cluster) "topology1")))
        (is (= "Running - Fully Scheduled by DefaultResourceAwareStrategy" (.get (.getStatusMap cluster) "topology2")))))))

;; Automated tests for heterogeneous cluster
(deftest test-heterogeneous-cluster
  (let [supers (into {} (for [super [(SupervisorDetails. (str "id" 0) (str "host" 0) (list )
                                                         (map int (list 1 2 3 4))
                                                         {Config/SUPERVISOR_MEMORY_CAPACITY_MB 4096.0
                                                          Config/SUPERVISOR_CPU_CAPACITY 800.0})
                                     (SupervisorDetails. (str "id" 1) (str "host" 1) (list )
                                                         (map int (list 1 2 3 4))
                                                         {Config/SUPERVISOR_MEMORY_CAPACITY_MB 1024.0
                                                          Config/SUPERVISOR_CPU_CAPACITY 200.0})]]
                          {(.getId super) super}))
        builder1 (TopologyBuilder.)  ;; topo1 has one single huge task that can not be handled by the small-super
        _ (doto (.setSpout builder1 "spout1" (TestWordSpout.) 1)
            (.setMemoryLoad 2000.0 48.0)
            (.setCPULoad 300.0))
        storm-topology1 (.createTopology builder1)
        topology1 (TopologyDetails. "topology1"
                    {TOPOLOGY-NAME "topology-name-1"
                     TOPOLOGY-SUBMITTER-USER "userC"
                     TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 128.0
                     TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                     TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT 10.0
                     TOPOLOGY-WORKER-MAX-HEAP-SIZE-MB 8192.0
                     TOPOLOGY-PRIORITY 0
                     TOPOLOGY-SCHEDULER-STRATEGY DEFAULT_SCHEDULING_STRATEGY}
                    storm-topology1
                    1
                    (mk-ed-map [["spout1" 0 1]]))
        builder2 (TopologyBuilder.)  ;; topo2 has 4 large tasks
        _ (doto (.setSpout builder2 "spout2" (TestWordSpout.) 4)
            (.setMemoryLoad 500.0 12.0)
            (.setCPULoad 100.0))
        storm-topology2 (.createTopology builder2)
        topology2 (TopologyDetails. "topology2"
                    {TOPOLOGY-NAME "topology-name-2"
                     TOPOLOGY-SUBMITTER-USER "userC"
                     TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 128.0
                     TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                     TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT 10.0
                     TOPOLOGY-WORKER-MAX-HEAP-SIZE-MB 8192.0
                     TOPOLOGY-PRIORITY 0
                     TOPOLOGY-SCHEDULER-STRATEGY DEFAULT_SCHEDULING_STRATEGY}
                    storm-topology2
                    2
                    (mk-ed-map [["spout2" 0 4]]))
        builder3 (TopologyBuilder.) ;; topo3 has 4 medium tasks, launching topo 1-3 together requires the same mem as the cluster's mem capacity (5G)
        _ (doto (.setSpout builder3 "spout3" (TestWordSpout.) 4)
            (.setMemoryLoad 200.0 56.0)
            (.setCPULoad 20.0))
        storm-topology3 (.createTopology builder3)
        topology3 (TopologyDetails. "topology3"
                    {TOPOLOGY-NAME "topology-name-3"
                     TOPOLOGY-SUBMITTER-USER "userC"
                     TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 128.0
                     TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                     TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT 10.0
                     TOPOLOGY-WORKER-MAX-HEAP-SIZE-MB 8192.0
                     TOPOLOGY-PRIORITY 0
                     TOPOLOGY-SCHEDULER-STRATEGY DEFAULT_SCHEDULING_STRATEGY}
                    storm-topology3
                    2
                    (mk-ed-map [["spout3" 0 4]]))
        builder4 (TopologyBuilder.) ;; topo4 has 12 small tasks, each's mem req does not exactly divide a node's mem capacity
        _ (doto (.setSpout builder4 "spout4" (TestWordSpout.) 2)
            (.setMemoryLoad 100.0 0.0)
            (.setCPULoad 30.0))
        storm-topology4 (.createTopology builder4)
        topology4 (TopologyDetails. "topology4"
                    {TOPOLOGY-NAME "topology-name-4"
                     TOPOLOGY-SUBMITTER-USER "userC"
                     TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 128.0
                     TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                     TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT 10.0
                     TOPOLOGY-WORKER-MAX-HEAP-SIZE-MB 8192.0
                     TOPOLOGY-PRIORITY 0
                     TOPOLOGY-SCHEDULER-STRATEGY DEFAULT_SCHEDULING_STRATEGY}
                    storm-topology4
                    2
                    (mk-ed-map [["spout4" 0 12]]))
        builder5 (TopologyBuilder.) ;; topo5 has 40 small tasks, it should be able to exactly use up both the cpu and mem in teh cluster
        _ (doto (.setSpout builder5 "spout5" (TestWordSpout.) 40)
            (.setMemoryLoad 100.0 28.0)
            (.setCPULoad 25.0))
        storm-topology5 (.createTopology builder5)
        topology5 (TopologyDetails. "topology5"
                    {TOPOLOGY-NAME "topology-name-5"
                     TOPOLOGY-SUBMITTER-USER "userC"
                     TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 128.0
                     TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                     TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT 10.0
                     TOPOLOGY-WORKER-MAX-HEAP-SIZE-MB 8192.0
                     TOPOLOGY-PRIORITY 0
                     TOPOLOGY-SCHEDULER-STRATEGY DEFAULT_SCHEDULING_STRATEGY}
                    storm-topology5
                    2
                    (mk-ed-map [["spout5" 0 40]]))
        epsilon 0.000001
        topologies (Topologies. (to-top-map [topology1 topology2]))]

    (testing "Launch topo 1-3 together, it should be able to use up either mem or cpu resource due to exact division"
      (let [cluster (Cluster. (nimbus/standalone-nimbus) supers {}
                              {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                               "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping"})
            topologies (Topologies. (to-top-map [topology1 topology2 topology3]))
            scheduler (ResourceAwareScheduler.)
            _ (.prepare scheduler {RESOURCE-AWARE-SCHEDULER-EVICTION-STRATEGY DEFAULT_EVICTION_STRATEGY
                                   RESOURCE-AWARE-SCHEDULER-PRIORITY-STRATEGY DEFAULT_PRIORITY_STRATEGY})
            _ (.schedule scheduler topologies cluster)
            super->mem-usage (get-super->mem-usage cluster topologies)
            super->cpu-usage (get-super->cpu-usage cluster topologies)]
        (is (= "Running - Fully Scheduled by DefaultResourceAwareStrategy" (.get (.getStatusMap cluster) "topology1")))
        (is (= "Running - Fully Scheduled by DefaultResourceAwareStrategy" (.get (.getStatusMap cluster) "topology2")))
        (is (= "Running - Fully Scheduled by DefaultResourceAwareStrategy" (.get (.getStatusMap cluster) "topology3")))
        (doseq [super (.values supers)]
          (let [mem-avail (.getTotalMemory super)
                mem-used (.get super->mem-usage super)
                cpu-avail (.getTotalCPU super)
                cpu-used (.get super->cpu-usage super)]
            (is (or (<= (Math/abs (- mem-avail mem-used)) epsilon)
                    (<= (Math/abs (- cpu-avail cpu-used)) epsilon)))))))

    (testing "Launch topo 1, 2 and 4, they together request a little more mem than available, so one of the 3 topos will not be scheduled"
      (let [cluster (Cluster. (nimbus/standalone-nimbus) supers {}
                              {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                               "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping"})
            topologies (Topologies. (to-top-map [topology1 topology2 topology3]))
            scheduler (ResourceAwareScheduler.)
            _ (.prepare scheduler {RESOURCE-AWARE-SCHEDULER-EVICTION-STRATEGY DEFAULT_EVICTION_STRATEGY
                                   RESOURCE-AWARE-SCHEDULER-PRIORITY-STRATEGY DEFAULT_PRIORITY_STRATEGY})
            _ (.schedule scheduler topologies cluster)
                scheduled-topos (if (= "Running - Fully Scheduled by DefaultResourceAwareStrategy" (.get (.getStatusMap cluster) "topology1")) 1 0)
                scheduled-topos (+ scheduled-topos (if (= "Running - Fully Scheduled by DefaultResourceAwareStrategy" (.get (.getStatusMap cluster) "topology2")) 1 0))
                scheduled-topos (+ scheduled-topos (if (= "Running - Fully Scheduled by DefaultResourceAwareStrategy" (.get (.getStatusMap cluster) "topology4")) 1 0))]
            (is (= scheduled-topos 2)))) ;; only 2 topos will get (fully) scheduled

    (testing "Launch topo5 only, both mem and cpu should be exactly used up"
      (let [cluster (Cluster. (nimbus/standalone-nimbus) supers {}
                              {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                               "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping"})
            topologies (Topologies. (to-top-map [topology5]))
            scheduler (ResourceAwareScheduler.)
            _ (.prepare scheduler {RESOURCE-AWARE-SCHEDULER-EVICTION-STRATEGY DEFAULT_EVICTION_STRATEGY
                                   RESOURCE-AWARE-SCHEDULER-PRIORITY-STRATEGY DEFAULT_PRIORITY_STRATEGY})
            _ (.schedule scheduler topologies cluster)
            super->mem-usage (get-super->mem-usage cluster topologies)
            super->cpu-usage (get-super->cpu-usage cluster topologies)]
        (is (= "Running - Fully Scheduled by DefaultResourceAwareStrategy" (.get (.getStatusMap cluster) "topology5")))
        (doseq [super (.values supers)]
          (let [mem-avail (.getTotalMemory super)
                mem-used (.get super->mem-usage super)
                cpu-avail (.getTotalCPU ^SupervisorDetails super)
                cpu-used (.get super->cpu-usage super)]
            (is (and (<= (Math/abs (- mem-avail mem-used)) epsilon)
                    (<= (Math/abs (- cpu-avail cpu-used)) epsilon)))))))))

(deftest test-topology-worker-max-heap-size
  (let [supers (gen-supervisors 2 2)]
    (testing "test if RAS will spread executors across mulitple workers based on the set limit for a worker used by the topology")
    (let [cluster (Cluster. (nimbus/standalone-nimbus) supers {}
                              {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                               "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping"})
          scheduler (ResourceAwareScheduler.)
          builder1 (TopologyBuilder.)
          _ (.setSpout builder1 "spout1" (TestWordSpout.) 2)
          storm-topology1 (.createTopology builder1)
          topology1 (TopologyDetails. "topology1"
                      {TOPOLOGY-NAME "topology-name-1"
                       TOPOLOGY-SUBMITTER-USER "userA"
                       TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 128.0
                       TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                       TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT 10.0
                       TOPOLOGY-WORKER-MAX-HEAP-SIZE-MB 128.0
                       TOPOLOGY-PRIORITY 0
                       TOPOLOGY-SCHEDULER-STRATEGY DEFAULT_SCHEDULING_STRATEGY}
                      storm-topology1
                      1
                      (mk-ed-map [["spout1" 0 4]]))
          topologies (Topologies. (to-top-map [topology1]))]
      (.prepare scheduler {RESOURCE-AWARE-SCHEDULER-EVICTION-STRATEGY DEFAULT_EVICTION_STRATEGY
                           RESOURCE-AWARE-SCHEDULER-PRIORITY-STRATEGY DEFAULT_PRIORITY_STRATEGY})
      (.schedule scheduler topologies cluster)
      (is (= (.get (.getStatusMap cluster) "topology1") "Running - Fully Scheduled by DefaultResourceAwareStrategy"))
      (is (= (.getAssignedNumWorkers cluster topology1) 4)))
    (testing "test when no more workers are available due to topology worker max heap size limit but there is memory is still available")
    (let [cluster (Cluster. (nimbus/standalone-nimbus) supers {}
                              {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                               "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping"})
          scheduler (ResourceAwareScheduler.)
          builder1 (TopologyBuilder.)
          _ (.setSpout builder1 "spout1" (TestWordSpout.) 2)
          storm-topology1 (.createTopology builder1)
          topology1 (TopologyDetails. "topology1"
                      {TOPOLOGY-NAME "topology-name-1"
                       TOPOLOGY-SUBMITTER-USER "userC"
                       TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 128.0
                       TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                       TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT 10.0
                       TOPOLOGY-WORKER-MAX-HEAP-SIZE-MB 128.0
                       TOPOLOGY-PRIORITY 0
                       TOPOLOGY-SCHEDULER-STRATEGY DEFAULT_SCHEDULING_STRATEGY}
                      storm-topology1
                      1
                      (mk-ed-map [["spout1" 0 5]]))
          topologies (Topologies. (to-top-map [topology1]))]
      (.prepare scheduler {RESOURCE-AWARE-SCHEDULER-EVICTION-STRATEGY DEFAULT_EVICTION_STRATEGY
                           RESOURCE-AWARE-SCHEDULER-PRIORITY-STRATEGY DEFAULT_PRIORITY_STRATEGY})
      (.schedule scheduler topologies cluster)
      ;;spout1 is going to contain 5 executors that needs scheduling. Each of those executors has a memory requirement of 128.0 MB
      ;;The cluster contains 4 free WorkerSlots. For this topolology each worker is limited to a max heap size of 128.0
      ;;Thus, one executor not going to be able to get scheduled thus failing the scheduling of this topology and no executors of this topology will be scheduleded
      (is (= (.size (.getUnassignedExecutors cluster topology1)) 5))
      (is (= (.get (.getStatusMap cluster) "topology1")  "Not enough resources to schedule - 0/5 executors scheduled")))

    (let [cluster (Cluster. (nimbus/standalone-nimbus) supers {}
                             {STORM-NETWORK-TOPOGRAPHY-PLUGIN
                              "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping"})
          cluster (LocalCluster.)
          builder1 (TopologyBuilder.)
          _ (.setSpout builder1 "spout1" (TestWordSpout.) 2)
          storm-topology1 (.createTopology builder1)
          conf  {TOPOLOGY-NAME "topology-name-1"
                 TOPOLOGY-SUBMITTER-USER "userC"
                 TOPOLOGY-COMPONENT-RESOURCES-ONHEAP-MEMORY-MB 129.0
                 TOPOLOGY-COMPONENT-RESOURCES-OFFHEAP-MEMORY-MB 0.0
                 TOPOLOGY-COMPONENT-CPU-PCORE-PERCENT 10.0
                 TOPOLOGY-WORKER-MAX-HEAP-SIZE-MB 128.0
                 TOPOLOGY-PRIORITY 0
                 TOPOLOGY-SCHEDULER-STRATEGY DEFAULT_SCHEDULING_STRATEGY}
          topology1 (TopologyDetails. "topology1"
                      conf
                      storm-topology1
                      1
                      (mk-ed-map [["spout1" 0 5]]))
          topologies (Topologies. (to-top-map [topology1]))]
      (is (thrown? IllegalArgumentException
            (StormSubmitter/submitTopologyWithProgressBar "test" conf storm-topology1)))

  )))
