(ns backtype.storm.tick-tuple-test
  (:use [clojure test])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common]))

(bootstrap)

(defbolt noop-bolt ["tuple"] {:prepare true}
  [conf context collector]
  (bolt
   (execute [tuple])))

(defspout noop-spout ["tuple"]
  [conf context collector]
  (spout
   (nextTuple [])))

(deftest test-tick-tuple-works-with-system-bolt
  (with-simulated-time-local-cluster [cluster]
    (let [topology (thrift/mk-topology
                    {"1" (thrift/mk-spout-spec noop-spout)}
                    {"2" (thrift/mk-bolt-spec {"1" ["tuple"]} noop-bolt)})]
      (try
        (submit-local-topology (:nimbus cluster)
                               "test"
                               {TOPOLOGY-TICK-TUPLE-FREQ-SECS 1}
                               topology)
        (advance-cluster-time cluster 2)
        ;; if reaches here, it means everything works ok.
        (is true)
        (catch Exception e
          (is false))))))



