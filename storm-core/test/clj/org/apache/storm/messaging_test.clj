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
(ns org.apache.storm.messaging-test
  (:use [clojure test])
  (:import [org.apache.storm.testing TestWordCounter TestWordSpout TestGlobalCount TestEventLogSpout TestEventOrderCheckBolt])
  (:use [org.apache.storm testing config])
  (:use [org.apache.storm.daemon common])
  (:require [org.apache.storm [thrift :as thrift]]))

(deftest test-local-transport
  (doseq [transport-on? [false true]] 
    (with-simulated-time-local-cluster [cluster :supervisors 1 :ports-per-supervisor 2
                                        :daemon-conf {TOPOLOGY-WORKERS 2
                                                      STORM-LOCAL-MODE-ZMQ 
                                                      (if transport-on? true false) 
                                                      STORM-MESSAGING-TRANSPORT 
                                                      "org.apache.storm.messaging.netty.Context"}]
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
        (is (= (* 6 4) (.size (read-tuples results "2"))))))))

(extend-type TestEventLogSpout
  CompletableSpout
  (exhausted? [this]
    (-> this .completed))
  (cleanup [this]
    (.cleanup this))
  (startup [this]
    ))
