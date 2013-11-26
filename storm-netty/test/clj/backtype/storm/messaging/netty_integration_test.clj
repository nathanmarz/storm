(ns backtype.storm.messaging.netty-integration-test
  (:use [clojure test])
  (:import [backtype.storm.messaging TransportFactory])
  (:import [backtype.storm.testing TestWordSpout TestGlobalCount])
  (:use [backtype.storm bootstrap testing util]))

(bootstrap)

(deftest test-integration
  (with-simulated-time-local-cluster [cluster :supervisors 4 :supervisor-slot-port-min 6710
                                      :daemon-conf {STORM-LOCAL-MODE-ZMQ true 
                                                    STORM-MESSAGING-TRANSPORT  "backtype.storm.messaging.netty.Context"
                                                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024000
                                                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                                                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000 
                                                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                                                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                                                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                                                    }]
    (let [topology (thrift/mk-topology
                     {"1" (thrift/mk-spout-spec (TestWordSpout. true) :parallelism-hint 4)}
                     {"2" (thrift/mk-bolt-spec {"1" :shuffle} (TestGlobalCount.)
                                               :parallelism-hint 6)})
          results (complete-topology cluster
                                     topology
                                     ;; important for test that
                                     ;; #tuples = multiple of 4 and 6
                                     :storm-conf {TOPOLOGY-WORKERS 3}
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
               (read-tuples results "2"))))))
