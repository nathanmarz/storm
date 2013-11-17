(ns backtype.storm.utils.ThriftTopologyUtils-test
  (:import (backtype.storm.generated StormTopology SpoutSpec Bolt StateSpoutSpec)
           (backtype.storm.utils Utils ThriftTopologyUtils)
           (java.nio ByteBuffer))
  (:use [clojure test]))

(defn- mk-worker-hooks []
  ;; Does the binary data here even matter? I'm just putting junk bytes
  ;; in place of the serialized worker hook object
  (->> [13 12 11 10 9 8 7]
      (map byte)
      (byte-array)
      (ByteBuffer/wrap)
      (conj '())))

(def spout-key "spout1")
(def bolt-key "bolt1")
(def state-spout-key "state-spout1")

(defn- all-keys-found? [ids]
  (every? #((set ids) %) [spout-key bolt-key state-spout-key]))

(defn- mk-topo [with-hooks?]
  (let [spouts {spout-key (SpoutSpec.)}
        bolts {bolt-key (Bolt.)}
        state-spouts {state-spout-key (StateSpoutSpec.)}
        topo (StormTopology. spouts bolts state-spouts)]
    (when with-hooks?
      (.set_worker_hooks topo (mk-worker-hooks)))
    topo))

(deftest test-getComponentIds-with-no-worker-hooks
  (def ids (ThriftTopologyUtils/getComponentIds (mk-topo false)))
  (is (all-keys-found? ids)))

(deftest test-getComponentIds-with-worker-hooks
  (def ids (ThriftTopologyUtils/getComponentIds (mk-topo true)))
  (is (all-keys-found? ids)))
