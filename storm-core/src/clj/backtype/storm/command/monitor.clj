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
(ns backtype.storm.command.monitor
  (:use [clojure.tools.cli :only [cli]])
  (:use [backtype.storm.thrift :only [with-configured-nimbus-connection]])
  (:import [backtype.storm.utils Monitor])
  (:gen-class)
 )

(defn -main [& args]
  (let [[{interval :interval component :component stream :stream watch :watch} [name] _]
        (cli args ["-i" "--interval" :default 4 :parse-fn #(Integer/parseInt %)]
          ["-m" "--component" :default nil]
          ["-s" "--stream" :default "default"]
          ["-w" "--watch" :default "emitted"])
        mon (Monitor.)]
    (if interval (.set_interval mon interval))
    (if name (.set_topology mon name))
    (if component (.set_component mon component))
    (if stream (.set_stream mon stream))
    (if watch (.set_watch mon watch))
    (with-configured-nimbus-connection nimbus
      (.metrics mon nimbus)
      )))
