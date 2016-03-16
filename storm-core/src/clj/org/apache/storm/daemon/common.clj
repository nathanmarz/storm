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
(ns org.apache.storm.daemon.common
  (:use [org.apache.storm log config util])
  (:require [clojure.set :as set])
  (:import (org.apache.storm.task WorkerTopologyContext)
           (org.apache.storm.utils Utils ConfigUtils)
           (java.io InterruptedIOException)))

;; the task id is the virtual port
;; node->host is here so that tasks know who to talk to just from assignment
;; this avoid situation where node goes down and task doesn't know what to do information-wise
(defrecord Assignment [master-code-dir node->host executor->node+port executor->start-time-secs worker->resources])


;; component->executors is a map from spout/bolt id to number of executors for that component
(defrecord StormBase [storm-name launch-time-secs status num-workers component->executors owner topology-action-options prev-status component->debug])

(defrecord SupervisorInfo [time-secs hostname assignment-id used-ports meta scheduler-meta uptime-secs version resources-map])

(defrecord ExecutorStats [^long processed
                          ^long acked
                          ^long emitted
                          ^long transferred
                          ^long failed])

(defn new-executor-stats []
  (ExecutorStats. 0 0 0 0 0))

(defmacro defserverfn [name & body]
  `(let [exec-fn# (fn ~@body)]
    (defn ~name [& args#]
      (try-cause
        (apply exec-fn# args#)
      (catch InterruptedIOException e#
        (throw e#))
      (catch InterruptedException e#
        (throw e#))
      (catch Throwable t#
        (log-error t# "Error on initialization of server " ~(str name))
        (Utils/exitProcess 13 "Error on initialization")
        )))))

(defn worker-context [worker]
  (WorkerTopologyContext. (:system-topology worker)
                          (:storm-conf worker)
                          (:task->component worker)
                          (:component->sorted-tasks worker)
                          (:component->stream->fields worker)
                          (:storm-id worker)
                          (ConfigUtils/supervisorStormResourcesPath
                            (ConfigUtils/supervisorStormDistRoot (:conf worker) (:storm-id worker)))
                          (ConfigUtils/workerPidsRoot (:conf worker) (:worker-id worker))
                          (:port worker)
                          (:task-ids worker)
                          (:default-shared-resources worker)
                          (:user-shared-resources worker)
                          ))
