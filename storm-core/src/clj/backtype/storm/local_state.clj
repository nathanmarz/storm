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
(ns backtype.storm.local-state
  (:use [backtype.storm log util])
  (:import [backtype.storm.generated StormTopology
            InvalidTopologyException GlobalStreamId
            LSSupervisorId LSApprovedWorkers
            LSSupervisorAssignments LocalAssignment
            ExecutorInfo LSWorkerHeartbeat])
  (:import [backtype.storm.utils LocalState]))

(def LS-WORKER-HEARTBEAT "worker-heartbeat")
(def LS-ID "supervisor-id")
(def LS-LOCAL-ASSIGNMENTS "local-assignments")
(def LS-APPROVED-WORKERS "approved-workers")

(defn ls-supervisor-id!
  [^LocalState local-state ^String id]
    (.put local-state LS-ID (LSSupervisorId. id)))

(defn ls-supervisor-id
  [^LocalState local-state]
  (if-let [super-id (.get local-state LS-ID)]
    (.get_supervisor_id super-id)))

(defn ls-approved-workers!
  [^LocalState local-state workers]
    (.put local-state LS-APPROVED-WORKERS (LSApprovedWorkers. workers)))

(defn ls-approved-workers
  [^LocalState local-state]
  (if-let [tmp (.get local-state LS-APPROVED-WORKERS)]
    (into {} (.get_approved_workers tmp))))

(defn ->ExecutorInfo
  [[low high]] (ExecutorInfo. low high))

(defn ->ExecutorInfo-list
  [executors]
  (map ->ExecutorInfo executors))

(defn ->executor-list
  [executors]
  (into [] 
    (for [exec-info executors] 
      [(.get_task_start exec-info) (.get_task_end exec-info)])))

(defn ->LocalAssignment
  [{storm-id :storm-id executors :executors}]
  (LocalAssignment. storm-id (->ExecutorInfo-list executors)))

(defn mk-local-assignment
  [storm-id executors]
  {:storm-id storm-id :executors executors})

(defn ->local-assignment
  [^LocalAssignment thrift-local-assignment]
    (mk-local-assignment
      (.get_topology_id thrift-local-assignment)
      (->executor-list (.get_executors thrift-local-assignment))))

(defn ls-local-assignments!
  [^LocalState local-state assignments]
    (let [local-assignment-map (map-val ->LocalAssignment assignments)]
    (.put local-state LS-LOCAL-ASSIGNMENTS 
          (LSSupervisorAssignments. local-assignment-map))))

(defn ls-local-assignments
  [^LocalState local-state]
    (if-let [thrift-local-assignments (.get local-state LS-LOCAL-ASSIGNMENTS)]
      (map-val
        ->local-assignment
        (.get_assignments thrift-local-assignments))))

(defn ls-worker-heartbeat!
  [^LocalState local-state time-secs storm-id executors port]
  (.put local-state LS-WORKER-HEARTBEAT (LSWorkerHeartbeat. time-secs storm-id (->ExecutorInfo-list executors) port) false))

(defn ls-worker-heartbeat 
  [^LocalState local-state]
  (if-let [worker-hb (.get local-state LS-WORKER-HEARTBEAT)]
    {:time-secs (.get_time_secs worker-hb)
     :storm-id (.get_topology_id worker-hb)
     :executors (->executor-list (.get_executors worker-hb))
     :port (.get_port worker-hb)}))

