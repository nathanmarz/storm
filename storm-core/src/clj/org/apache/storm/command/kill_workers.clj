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
(ns org.apache.storm.command.kill-workers
  (:import [java.io File])
  (:use [org.apache.storm.daemon common])
  (:use [org.apache.storm util config])
  (:require [org.apache.storm.daemon
             [supervisor :as supervisor]])
  (:gen-class))

(defn -main 
  "Construct the supervisor-data from scratch and kill the workers on this supervisor"
  [& args]
  (let [conf (read-storm-config)
        conf (assoc conf STORM-LOCAL-DIR (. (File. (conf STORM-LOCAL-DIR)) getCanonicalPath))
        isupervisor (supervisor/standalone-supervisor)
        supervisor-data (supervisor/supervisor-data conf nil isupervisor)
        ids (supervisor/my-worker-ids conf)]
    (doseq [id ids]
      (supervisor/shutdown-worker supervisor-data id))))
