(ns backtype.storm.daemon.worker-test
  (:import (backtype.storm.generated StormTopology StateSpoutSpec Bolt SpoutSpec)
           (java.nio ByteBuffer)
           (backtype.storm.topology IWorkerHook TestWorkerHook)
           (backtype.storm.utils Utils))
  (:use (clojure test)
        (backtype.storm.daemon worker)))

;(def worker-hook-state (atom {}))
;
;(def worker-hook
;  (reify IWorkerHook
;    (start [this conf context task-ids]
;      (reset! worker-hook-state conf))))

(def spout-key "spout1")
(def bolt-key "bolt1")
(def state-spout-key "state-spout1")
(def worker-hook (-> (TestWorkerHook.)
                     (Utils/serialize)
                     (ByteBuffer/wrap)))

(defn- mk-topo []
  (let [spouts {spout-key (SpoutSpec.)}
        bolts {bolt-key (Bolt.)}
        state-spouts {state-spout-key (StateSpoutSpec.)}
        topo (StormTopology. spouts bolts state-spouts)
        _ (.set_worker_hooks topo (list worker-hook))]
    topo))

;; FIXME Have to mock worker-context behavior
(deftest test-start-worker-hooks
    (let [worker {:topology (mk-topo)}]
    (start-worker-hooks worker)))