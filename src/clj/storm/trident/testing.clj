(ns storm.trident.testing
  (:import [storm.trident.testing FeederBatchSpout MemoryMapState MemoryMapState$Factory])
  (:import [backtype.storm LocalDRPC])
  (:import [backtype.storm.tuple Fields])
  (:import [backtype.storm.generated KillOptions])
  (:require [backtype.storm [testing :as t]])
  (:use [backtype.storm util])
  )

(defn local-drpc []
  (LocalDRPC.))

(defn exec-drpc [^LocalDRPC drpc function-name args]
  (let [res (.execute drpc function-name args)]
    (from-json res)))

(defn feeder-spout [fields]
  (FeederBatchSpout. fields))

(defn feed [feeder tuples]
  (.feed feeder tuples))

(defn fields [& fields]
  (Fields. fields))

(defn memory-map-state []
  (MemoryMapState$Factory.))

(defmacro with-drpc [[drpc] & body]
  `(let [~drpc (backtype.storm.LocalDRPC.)]
     ~@body
     (.shutdown ~drpc)
     ))

(defn with-topology* [cluster topo body-fn]
  (t/submit-local-topology (:nimbus cluster) "tester" {} (.build topo))
  (body-fn)
  (.killTopologyWithOpts (:nimbus cluster) "tester" (doto (KillOptions.) (.set_wait_secs 0)))
  )

(defmacro with-topology [[cluster topo] & body]
  `(with-topology* ~cluster ~topo (fn [] ~@body)))

(defn bootstrap-imports []
  (import 'backtype.storm.LocalDRPC)
  (import 'storm.trident.TridentTopology)
  (import '[storm.trident.operation.builtin Count Sum Equals MapGet Debug FilterNull FirstN])
  )
