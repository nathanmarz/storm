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

(ns org.apache.storm.process-simulator
  (:use [org.apache.storm log util]))

(def pid-counter (mk-counter))

(def process-map (atom {}))

(def kill-lock (Object.))

(defn register-process [pid shutdownable]
  (swap! process-map assoc pid shutdownable))

(defn process-handle
  [pid]
  (@process-map pid))

(defn all-processes
  []
  (vals @process-map))

(defn kill-process
  "Uses `locking` in case cluster shuts down while supervisor is
  killing a task"
  [pid]
  (locking kill-lock
    (log-message "Killing process " pid)
    (let [shutdownable (process-handle pid)]
      (swap! process-map dissoc pid)
      (when shutdownable
        (.shutdown shutdownable)))))

(defn kill-all-processes
  []
  (doseq [pid (keys @process-map)]
    (kill-process pid)))
