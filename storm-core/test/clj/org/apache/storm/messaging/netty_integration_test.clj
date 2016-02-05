;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns org.apache.storm.messaging.netty-integration-test
  (:use [clojure test])
  (:import [org.apache.storm.messaging TransportFactory])
  (:import [org.apache.storm.testing TestWordSpout TestGlobalCount])
  (:use [org.apache.storm testing util config])
  (:require [org.apache.storm [thrift :as thrift]]))

(deftest test-integration
  (with-simulated-time-local-cluster [cluster :supervisors 4 :supervisor-slot-port-min 6710
                                      :daemon-conf {STORM-LOCAL-MODE-ZMQ true 
                                                    STORM-MESSAGING-TRANSPORT  "org.apache.storm.messaging.netty.Context"
                                                    STORM-MESSAGING-NETTY-AUTHENTICATION false
                                                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024000
                                                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                                                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000 
                                                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                                                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                                                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1}]
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
        (is (= (* 6 4) (.size (read-tuples results "2")))))))
