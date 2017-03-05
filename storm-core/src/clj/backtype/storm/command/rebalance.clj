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
(ns backtype.storm.command.rebalance
  (:use [clojure.tools.cli :only [cli]])
  (:use [backtype.storm thrift config log])
  (:import [backtype.storm.generated RebalanceOptions])
  (:gen-class))

(defn- parse-executor [^String s]
  (let [eq-pos (.lastIndexOf s "=")
        name (.substring s 0 eq-pos)
        amt (.substring s (inc eq-pos))]
    {name (Integer/parseInt amt)}
    ))

(defn -main [& args] 
  (let [[{wait :wait executor :executor num-workers :num-workers} [name] _]
                  (cli args ["-w" "--wait" :default nil :parse-fn #(Integer/parseInt %)]
                            ["-n" "--num-workers" :default nil :parse-fn #(Integer/parseInt %)]
                            ["-e" "--executor" :combine-fn merge :parse-fn parse-executor])
        opts (RebalanceOptions.)]
    (if wait (.set_wait_secs opts wait))
    (if executor (.set_num_executors opts executor))
    (if num-workers (.set_num_workers opts num-workers))
    (with-configured-nimbus-connection nimbus
      (.rebalance nimbus name opts)
      (log-message "Topology " name " is rebalancing")
      )))
