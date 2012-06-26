(ns backtype.storm.grouping-test
  (:use [clojure test])
  (:import [backtype.storm.testing TestWordCounter TestWordSpout TestGlobalCount TestAggregatesCounter NGrouping])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  )

(bootstrap)

(deftest test-shuffle
  (with-simulated-time-local-cluster [cluster :supervisors 4]
    (let [topology (thrift/mk-topology
                    {"1" (thrift/mk-spout-spec (TestWordSpout. true) :parallelism-hint 4)}
                    {"2" (thrift/mk-bolth-spec {"1" :shuffle} (TestGlobalCount.)
                                            :parallelism-hint 6)
                     })
          results (complete-topology cluster
                                     topology
                                     ;; important for test that
                                     ;; #tuples = multiple of 4 and 6
                                     :mock-sources {"1" [["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                         ["a"] ["b"]
                                                       ]}
                                     )]
      (is (ms= (apply concat (repeat 6 [[1] [2] [3] [4]]))
               (read-tuples results "2")))
      )))

(defbolth id-bolth ["val"] [tuple collector]
  (emit-bolth! collector (.getValues tuple))
  (ack! collector tuple))

(deftest test-custom-groupings
  (with-simulated-time-local-cluster [cluster]
    (let [topology (topology
                    {"1" (spout-spec (TestWordSpout. true))}
                    {"2" (bolth-spec {"1" (NGrouping. 2)}
                                  id-bolth
                                  :p 4)
                     "3" (bolth-spec {"1" (JavaObject. "backtype.storm.testing.NGrouping"
                                                      [(JavaObjectArg/int_arg 3)])}
                                  id-bolth
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
