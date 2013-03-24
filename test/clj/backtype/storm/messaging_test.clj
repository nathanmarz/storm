(ns backtype.storm.messaging-test
  (:use [clojure test])
  (:import [backtype.storm.testing TestWordCounter TestWordSpout TestGlobalCount])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  )

(bootstrap)

(deftest test-messaging-plugins
  (doseq [plugin-id [0 1]]
    (with-simulated-time-local-cluster [cluster :supervisors 4
                                        :daemon-conf (if (= plugin-id 0) {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.local.TransportPlugin"}
                                                       {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.zmq.TransportPlugin"})]
    (let [topology (thrift/mk-topology
                    {"1" (thrift/mk-spout-spec (TestWordSpout. true) :parallelism-hint 4)}
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
