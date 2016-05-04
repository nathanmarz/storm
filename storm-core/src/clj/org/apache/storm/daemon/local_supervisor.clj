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
(ns org.apache.storm.daemon.local-supervisor
  (:import [org.apache.storm.daemon.supervisor SyncProcessEvent SupervisorData Supervisor SupervisorUtils]
           [org.apache.storm.utils Utils ConfigUtils]
           [org.apache.storm ProcessSimulator])
  (:use [org.apache.storm.daemon common]
        [org.apache.storm log])
  (:require [org.apache.storm.daemon [worker :as worker] ])
  (:require [clojure.string :as str])
  (:gen-class))

(defn launch-local-worker [supervisorData stormId port workerId resources]
  (let [conf (.getConf supervisorData)
         pid (Utils/uuid)
        worker (worker/mk-worker conf
                 (.getSharedContext supervisorData)
                 stormId
                 (.getAssignmentId supervisorData)
                 (int port)
                 workerId)]
    (ConfigUtils/setWorkerUserWSE conf workerId "")
    (ProcessSimulator/registerProcess pid worker)
    (.put (.getWorkerThreadPids supervisorData) workerId pid)))

(defn shutdown-local-worker [supervisorData worker-manager workerId]
  (log-message "shutdown-local-worker")
  (let [supervisor-id (.getSupervisorId supervisorData)
        worker-pids (.getWorkerThreadPids supervisorData)
        dead-workers (.getDeadWorkers supervisorData)]
    (.shutdownWorker worker-manager supervisor-id workerId worker-pids)
    (if (.cleanupWorker worker-manager workerId)
      (.remove dead-workers workerId))))

(defn local-process []
  "Create a local process event"
  (proxy [SyncProcessEvent] []
    (launchLocalWorker [supervisorData stormId port workerId resources]
      (launch-local-worker supervisorData stormId port workerId resources))
    (killWorker [supervisorData worker-manager workerId] (shutdown-local-worker supervisorData worker-manager workerId))))


(defserverfn mk-local-supervisor [conf shared-context isupervisor]
  (log-message "Starting local Supervisor with conf " conf)
  (if (not (ConfigUtils/isLocalMode conf))
    (throw
      (IllegalArgumentException. "Cannot start server in distrubuted mode!")))
  (let [local-process (local-process)
        supervisor-server (Supervisor.)]
    (.setLocalSyncProcess supervisor-server local-process)
    (.mkSupervisor supervisor-server conf shared-context isupervisor)))