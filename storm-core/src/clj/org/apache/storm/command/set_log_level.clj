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
(ns org.apache.storm.command.set-log-level
  (:use [clojure.tools.cli :only [cli]])
  (:use [org.apache.storm thrift log])
  (:import [org.apache.logging.log4j Level])
  (:import [org.apache.storm.generated LogConfig LogLevel LogLevelAction])
  (:gen-class))

(defn- get-storm-id
  "Get topology id for a running topology from the topology name."
  [nimbus name]
  (let [info (.getClusterInfo nimbus)
        topologies (.get_topologies info)
        topology (first (filter (fn [topo] (= name (.get_name topo))) topologies))]
    (if topology 
      (.get_id topology)
      (throw (.IllegalArgumentException (str name " is not a running topology"))))))

(defn- parse-named-log-levels [action]
  "Parses [logger name]=[level string]:[optional timeout],[logger name2]...

   e.g. ROOT=DEBUG:30
        root logger, debug for 30 seconds

        org.apache.foo=WARN
        org.apache.foo set to WARN indefinitely"
  (fn [^String s]
    (let [log-args (re-find #"(.*)=([A-Z]+):?(\d*)" s)
          name (if (= action LogLevelAction/REMOVE) s (nth log-args 1))
          level (Level/toLevel (nth log-args 2))
          timeout-str (nth log-args 3)
          log-level (LogLevel.)]
      (if (= action LogLevelAction/REMOVE)
        (.set_action log-level action)
        (do
          (.set_action log-level action)
          (.set_target_log_level log-level (.toString level))
          (.set_reset_log_level_timeout_secs log-level
            (Integer. (if (= timeout-str "") "0" timeout-str)))))
      {name log-level})))

(defn- merge-together [previous key val]
   (assoc previous key
      (if-let [oldval (get previous key)]
         (merge oldval val)
         val)))

(defn -main [& args]
  (let [[{log-setting :log-setting remove-log-setting :remove-log-setting} [name] _]
        (cli args ["-l" "--log-setting"
                   :parse-fn (parse-named-log-levels LogLevelAction/UPDATE)
                   :assoc-fn merge-together]
                  ["-r" "--remove-log-setting"
                   :parse-fn (parse-named-log-levels LogLevelAction/REMOVE)
                   :assoc-fn merge-together])
        log-config (LogConfig.)]
    (doseq [[log-name log-val] (merge log-setting remove-log-setting)]
      (.put_to_named_logger_level log-config log-name log-val))
    (log-message "Sent log config " log-config " for topology " name)
    (with-configured-nimbus-connection nimbus
      (.setLogConfig nimbus (get-storm-id nimbus name) log-config))))
