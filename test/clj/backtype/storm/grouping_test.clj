(ns backtype.storm.grouping-test
  (:use [clojure test])
  (:import [backtype.storm.testing TestWordCounter TestWordSpout TestGlobalCount TestAggregatesCounter NGrouping])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  )

(bootstrap)

(defbolt id-bolt ["val"] [tuple collector]
  (emit-bolt! collector (.getValues tuple))
  (ack! collector tuple))

(deftest test-custom-groupings
  (with-simulated-time-local-cluster [cluster]
    (let [topology (topology
                    {"1" (spout-spec (TestWordSpout. true))}
                    {"2" (bolt-spec {"1" (NGrouping. 2)}
                                  id-bolt
                                  :p 4)
                     "3" (bolt-spec {"1" (JavaObject. "backtype.storm.testing.NGrouping"
                                                      [(JavaObjectArg/int_arg 3)])}
                                  id-bolt
                                  :p 6)
                     })
          results (complete-topology cluster
                                     topology
                                     :mock-sources {"1" [["a"]
                                                        ["b"]
                                                        ]}
                                     )]
      (is (ms= [["a"] ["a"] ["b"] ["b"]]
               (read-tuples results "2")))
      (is (ms= [["a"] ["a"] ["a"] ["b"] ["b"] ["b"]]
               (read-tuples results "3")))
      )))
