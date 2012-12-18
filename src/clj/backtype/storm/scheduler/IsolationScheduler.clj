(ns backtype.storm.scheduler.IsolationScheduler
  (:use [backtype.storm util config])
  (:require [backtype.storm.scheduler.DefaultScheduler :as DefaultScheduler])
  (:import [java.util HashSet])
  (:import [backtype.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot SchedulerAssignment
            EvenScheduler ExecutorDetails])
  (:gen-class
    :init init
    :constructors {[] []}
    :state state 
    :implements [backtype.storm.scheduler.IScheduler]))

(defn -init []
  [[] (container)])

(defn -prepare [this conf]
  (container-set! (.state this) conf))


(defn- compute-worker-specs "Returns list of sets of executors"
  [^TopologyDetails details]
  (->> (.getExecutorToComponent details)
       reverse-map
       (map second)
       (apply interleave-all)
       (partition-fixed (.getNumWorkers details))
       (map set)))

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
       (HashSet.)))

(defn isolated-topologies [conf topologies]
  (let [tset (-> conf (get ISOLATION-SCHEDULER-MACHINES) keys set)]
    (filter (fn [^TopologyDetails t] (contains? tset (.getName t)) topologies)
    )))

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

(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (let [conf (container-get (.state this))        
        orig-blacklist (HashSet. (.getBlacklistedHosts cluster))
        iso-topologies (isolated-topologies (.getTopologies topologies))
        topology-worker-specs (topology-worker-specs iso-topologies)
        topology-machine-distribution (topology-machine-distribution conf iso-topologies)]
        
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
  ;; get host -> all assignable worker slots for non-blacklisted machines (assigned or not assigned)
  ;; will then have a list of machines that need to be assigned (machine -> [topology, list of list of executors])
  ;; match each spec to a machine (who has the right number of workers), free everything else on that machine and assign those slots (do one topology at a time)
  ;; blacklist all machines who had production slots defined
  ;; log isolated topologies who weren't able to get enough slots / machines
  ;; run default scheduler on isolated topologies that didn't have enough slots + non-isolated topologies on remaining machines
  ;; set blacklist to what it was initially
