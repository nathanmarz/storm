(ns backtype.storm.daemon.common
  (:use [clojure.contrib.seq-utils :only [find-first]])
  (:use [backtype.storm log config util])
  (:import [backtype.storm.generated StormTopology])
  (:import [backtype.storm.utils Utils])
  (:require [backtype.storm.thrift :as thrift])
  )

(defn system-component? [id]
  (Utils/isSystemComponent id))

(def ACKER-COMPONENT-ID "__acker")
(def ACKER-INIT-STREAM-ID "__ack_init")
(def ACKER-ACK-STREAM-ID "__ack_ack")
(def ACKER-FAIL-STREAM-ID "__ack_fail")

;; the task id is the virtual port
;; node->host is here so that tasks know who to talk to just from assignment
;; this avoid situation where node goes down and task doesn't know what to do information-wise
(defrecord Assignment [master-code-dir node->host task->node+port task->start-time-secs])

(defrecord StormBase [storm-name launch-time-secs status])

(defrecord SupervisorInfo [time-secs hostname worker-ports uptime-secs])

(defrecord TaskInfo [component-id])

(defprotocol DaemonCommon
  (waiting? [this]))

(def LS-WORKER-HEARTBEAT "worker-heartbeat")

;; LocalState constants
(def LS-ID "supervisor-id")
(def LS-LOCAL-ASSIGNMENTS "local-assignments")
(def LS-APPROVED-WORKERS "approved-workers")



(defrecord WorkerHeartbeat [time-secs storm-id task-ids port])

(defrecord TaskStats [^long processed
                      ^long acked
                      ^long emitted
                      ^long transferred
                      ^long failed])

(defrecord TaskHeartbeat [time-secs uptime-secs stats])

(defn new-task-stats []
  (TaskStats. 0 0 0 0 0))

;technically this is only active task ids
(defn storm-task-ids [storm-cluster-state storm-id]
  (keys (:task->node+port (.assignment-info storm-cluster-state storm-id nil))))

(defn storm-task-info
  "Returns map from task -> component id"
  [storm-cluster-state storm-id]
  (let [task-ids (.task-ids storm-cluster-state storm-id)]
    (into {}
      (dofor [id task-ids]
        [id (:component-id (.task-info storm-cluster-state storm-id id))]
        ))))

(defn get-storm-id [storm-cluster-state storm-name]
  (let [active-storms (.active-storms storm-cluster-state)]
    (find-first
      #(= storm-name (:storm-name (.storm-base storm-cluster-state % nil)))
      active-storms)
    ))

(defn topology-bases [storm-cluster-state]
  (let [active-topologies (.active-storms storm-cluster-state)]
    (into {} 
          (dofor [id active-topologies]
                 [id (.storm-base storm-cluster-state id nil)]
                 ))
    ))

(defn validate-distributed-mode! [conf]
  (if (local-mode? conf)
      (throw
        (IllegalArgumentException. "Cannot start server in local mode!"))))

(defmacro defserverfn [name & body]
  `(let [exec-fn# (fn ~@body)]
    (defn ~name [& args#]
      (try
        (apply exec-fn# args#)
      (catch Throwable t#
        (log-error t# "Error on initialization of server " ~(str name))
        (halt-process! 13 "Error on initialization")
        )))))

(defn system-topology [storm-conf ^StormTopology topology]
  (let [ret (.deepCopy topology)
        bolt-ids (.. ret get_bolts keySet)
        spout-ids (.. ret get_spouts keySet)
        spout-inputs (apply merge
                        (for [id spout-ids]
                          {[id ACKER-INIT-STREAM-ID] ["id"]}
                          ))
        bolt-inputs (apply merge
                      (for [id bolt-ids]
                        {[id ACKER-ACK-STREAM-ID] ["id"]
                         [id ACKER-FAIL-STREAM-ID] ["id"]}
                        ))
        acker-bolt (thrift/mk-bolt-spec (merge spout-inputs bolt-inputs)
                                        (backtype.storm.acker.)
                                        :p (storm-conf TOPOLOGY-ACKERS))]
    (.put_to_bolts ret "__acker" acker-bolt)
    (dofor [[_ bolt] (.get_bolts ret)
            :let [common (.get_common bolt)]]
      (.put_to_streams ACKER-ACK-STREAM-ID (thrift/output-fields ["id" "ack-val"]))
      (.put_to_streams ACKER-FAIL-STREAM-ID (thrift/output-fields ["id"])))
    (dofor [[_ spout] (.get_spouts ret)
            :let [common (.get_common bolt)]]
      (.put_to_streams ACKER-INIT-STREAM-ID (thrift/output-fields ["id" "spout-task"])))
    ret
    ))
