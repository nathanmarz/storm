(ns backtype.storm.messaging-test
  (:use [clojure test])
  (:import [backtype.storm.testing TestWordCounter TestWordSpout TestGlobalCount])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  )

(bootstrap)

(deftest test-local-transport
  (doseq [transport-on? [false true]] 
    (with-simulated-time-local-cluster [cluster :supervisors 1 :ports-per-supervisor 2
                                        :daemon-conf {TOPOLOGY-WORKERS 2
                                                      STORM-LOCAL-MODE-ZMQ 
                                                      (if transport-on? true false) 
                                                      STORM-MESSAGING-TRANSPORT 
                                                      "backtype.storm.messaging.netty.Context"}]
      (let [topology (thrift/mk-topology
                       {"1" (thrift/mk-spout-spec (TestWordSpout. true) :parallelism-hint 2)}
                       {"2" (thrift/mk-bolt-spec {"1" :shuffle} (TestGlobalCount.)
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
                 (read-tuples results "2")))))))

