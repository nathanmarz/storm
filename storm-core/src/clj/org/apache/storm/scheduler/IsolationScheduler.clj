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
(ns org.apache.storm.scheduler.IsolationScheduler
  (:use [org.apache.storm util config log])
  (:require [org.apache.storm.scheduler.DefaultScheduler :as DefaultScheduler])
  (:import [java.util HashSet Set List LinkedList ArrayList Map HashMap])
  (:import [org.apache.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot SchedulerAssignment
            EvenScheduler ExecutorDetails])
  (:gen-class
    :init init
    :constructors {[] []}
    :state state 
    :implements [org.apache.storm.scheduler.IScheduler]))

(defn -init []
  [[] (container)])

(defn -prepare [this conf]
  (container-set! (.state this) conf))

(defn- compute-worker-specs "Returns mutable set of sets of executors"
  [^TopologyDetails details]
  (->> (.getExecutorToComponent details)
       reverse-map
       (map second)
       (apply concat)
       (map vector (repeat-seq (range (.getNumWorkers details))))
       (group-by first)
       (map-val #(map second %))
       vals
       (map set)
       (HashSet.)
       ))

(defn isolated-topologies [conf topologies]
  (let [tset (-> conf (get ISOLATION-SCHEDULER-MACHINES) keys set)]
    (filter (fn [^TopologyDetails t] (contains? tset (.getName t))) topologies)
    ))

;; map from topology id -> set of sets of executors
(defn topology-worker-specs [iso-topologies]
  (->> iso-topologies
       (map (fn [t] {(.getId t) (compute-worker-specs t)}))
       (apply merge)))

(defn machine-distribution [conf ^TopologyDetails topology]
  (let [name->machines (get conf ISOLATION-SCHEDULER-MACHINES)
        machines (get name->machines (.getName topology))
        workers (.getNumWorkers topology)]
    (-> (integer-divided workers machines)
        (dissoc 0)
        (HashMap.)
        )))

(defn topology-machine-distribution [conf iso-topologies]
  (->> iso-topologies
       (map (fn [t] {(.getId t) (machine-distribution conf t)}))
       (apply merge)))

(defn host-assignments [^Cluster cluster]
  (letfn [(to-slot-specs [^SchedulerAssignment ass]
            (->> ass
                 .getExecutorToSlot
                 reverse-map
                 (map (fn [[slot executors]]
                        [slot (.getTopologyId ass) (set executors)]))))]
  (->> cluster
       .getAssignments
       vals
       (mapcat to-slot-specs)
       (group-by (fn [[^WorkerSlot slot & _]] (.getHost cluster (.getNodeId slot))))
       )))

(defn- decrement-distribution! [^Map distribution value]
  (let [v (-> distribution (get value) dec)]
    (if (zero? v)
      (.remove distribution value)
      (.put distribution value v))))

;; returns list of list of slots, reverse sorted by number of slots
(defn- host-assignable-slots [^Cluster cluster]
  (-<> cluster
       .getAssignableSlots
       (group-by #(.getHost cluster (.getNodeId ^WorkerSlot %)) <>)
       (dissoc <> nil)
       (sort-by #(-> % second count -) <>)
       shuffle
       (LinkedList. <>)
       ))

(defn- host->used-slots [^Cluster cluster]
  (->> cluster
       .getUsedSlots
       (group-by #(.getHost cluster (.getNodeId ^WorkerSlot %)))
       ))

(defn- distribution->sorted-amts [distribution]
  (->> distribution
       (mapcat (fn [[val amt]] (repeat amt val)))
       (sort-by -)
       ))

(defn- allocated-topologies [topology-worker-specs]
  (->> topology-worker-specs
    (filter (fn [[_ worker-specs]] (empty? worker-specs)))
    (map first)
    set
    ))

(defn- leftover-topologies [^Topologies topologies filter-ids-set]
  (->> topologies
       .getTopologies
       (filter (fn [^TopologyDetails t] (not (contains? filter-ids-set (.getId t)))))
       (map (fn [^TopologyDetails t] {(.getId t) t}))
       (apply merge)
       (Topologies.)
       ))

;; for each isolated topology:
;;   compute even distribution of executors -> workers on the number of workers specified for the topology
;;   compute distribution of workers to machines
;; determine host -> list of [slot, topology id, executors]
;; iterate through hosts and: a machine is good if:
;;   1. only running workers from one isolated topology
;;   2. all workers running on it match one of the distributions of executors for that topology
;;   3. matches one of the # of workers
;; blacklist the good hosts and remove those workers from the list of need to be assigned workers
;; otherwise unassign all other workers for isolated topologies if assigned

(defn remove-elem-from-set! [^Set aset]
  (let [elem (-> aset .iterator .next)]
    (.remove aset elem)
    elem
    ))

;; get host -> all assignable worker slots for non-blacklisted machines (assigned or not assigned)
;; will then have a list of machines that need to be assigned (machine -> [topology, list of list of executors])
;; match each spec to a machine (who has the right number of workers), free everything else on that machine and assign those slots (do one topology at a time)
;; blacklist all machines who had production slots defined
;; log isolated topologies who weren't able to get enough slots / machines
;; run default scheduler on isolated topologies that didn't have enough slots + non-isolated topologies on remaining machines
;; set blacklist to what it was initially
(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (let [conf (container-get (.state this))        
        orig-blacklist (HashSet. (.getBlacklistedHosts cluster))
        iso-topologies (isolated-topologies conf (.getTopologies topologies))
        iso-ids-set (->> iso-topologies (map #(.getId ^TopologyDetails %)) set)
        topology-worker-specs (topology-worker-specs iso-topologies)
        topology-machine-distribution (topology-machine-distribution conf iso-topologies)
        host-assignments (host-assignments cluster)]
    (doseq [[host assignments] host-assignments]
      (let [top-id (-> assignments first second)
            distribution (get topology-machine-distribution top-id)
            ^Set worker-specs (get topology-worker-specs top-id)
            num-workers (count assignments)
            ]
        (if (and (contains? iso-ids-set top-id)
                 (every? #(= (second %) top-id) assignments)
                 (contains? distribution num-workers)
                 (every? #(contains? worker-specs (nth % 2)) assignments))
          (do (decrement-distribution! distribution num-workers)
              (doseq [[_ _ executors] assignments] (.remove worker-specs executors))
              (.blacklistHost cluster host))
          (doseq [[slot top-id _] assignments]
            (when (contains? iso-ids-set top-id)
              (.freeSlot cluster slot)
              ))
          )))
    
    (let [host->used-slots (host->used-slots cluster)
          ^LinkedList sorted-assignable-hosts (host-assignable-slots cluster)]
      ;; TODO: can improve things further by ordering topologies in terms of who needs the least workers
      (doseq [[top-id worker-specs] topology-worker-specs
              :let [amts (distribution->sorted-amts (get topology-machine-distribution top-id))]]
        (doseq [amt amts
                :let [[host host-slots] (.peek sorted-assignable-hosts)]]
          (when (and host-slots (>= (count host-slots) amt))
            (.poll sorted-assignable-hosts)
            (.freeSlots cluster (get host->used-slots host))
            (doseq [slot (take amt host-slots)
                    :let [executors-set (remove-elem-from-set! worker-specs)]]
              (.assign cluster slot top-id executors-set))
            (.blacklistHost cluster host))
          )))
    
    (let [failed-iso-topologies (->> topology-worker-specs
                                  (mapcat (fn [[top-id worker-specs]]
                                    (if-not (empty? worker-specs) [top-id])
                                    )))]
      (if (empty? failed-iso-topologies)
        ;; run default scheduler on non-isolated topologies
        (-<> topology-worker-specs
             allocated-topologies
             (leftover-topologies topologies <>)
             (DefaultScheduler/default-schedule <> cluster))
        (do
          (log-warn "Unable to isolate topologies " (pr-str failed-iso-topologies) ". No machine had enough worker slots to run the remaining workers for these topologies. Clearing all other resources and will wait for enough resources for isolated topologies before allocating any other resources.")
          ;; clear workers off all hosts that are not blacklisted
          (doseq [[host slots] (host->used-slots cluster)]
            (if-not (.isBlacklistedHost cluster host)
              (.freeSlots cluster slots)
              )))
        ))
    (.setBlacklistedHosts cluster orig-blacklist)
    ))
