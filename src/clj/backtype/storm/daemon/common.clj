(ns backtype.storm.daemon.common
  (:use [clojure.contrib.seq-utils :only [find-first]])
  (:use [backtype.storm log config util])
  (:import [backtype.storm.generated StormTopology
            InvalidTopologyException GlobalStreamId])
  (:import [backtype.storm.utils Utils])
  (:require [backtype.storm.daemon.acker :as acker])
  (:require [backtype.storm.thrift :as thrift])
  )

(defn system-id? [id]
  (Utils/isSystemId id))

(def ACKER-COMPONENT-ID acker/ACKER-COMPONENT-ID)
(def ACKER-INIT-STREAM-ID acker/ACKER-INIT-STREAM-ID)
(def ACKER-ACK-STREAM-ID acker/ACKER-ACK-STREAM-ID)
(def ACKER-FAIL-STREAM-ID acker/ACKER-FAIL-STREAM-ID)

(def SYSTEM-STREAM-ID "__system")

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

(defn- validate-ids! [^StormTopology topology]
  (let [sets (map #(.getFieldValue topology %) thrift/STORM-TOPOLOGY-FIELDS)
        offending (apply any-intersection sets)]
    (if-not (empty? offending)
      (throw (InvalidTopologyException.
              (str "Duplicate component ids: " offending))))
    (doseq [f thrift/STORM-TOPOLOGY-FIELDS
            :let [obj-map (.getFieldValue topology f)]]
      (doseq [id (keys obj-map)]
        (if (system-id? id)
          (throw (InvalidTopologyException.
                  (str id " is not a valid component id")))))
      (doseq [obj (vals obj-map)
              id (-> obj .get_common .get_streams keys)]
        (if (system-id? id)
          (throw (InvalidTopologyException.
                  (str id " is not a valid stream id"))))))
    ))

(defn validate-basic! [^StormTopology topology]
  (validate-ids! topology)
  (doseq [f thrift/SPOUT-FIELDS
          obj (->> f (.getFieldValue topology) vals)]
    (if-not (empty? (-> obj .get_common .get_inputs))
      (throw (InvalidTopologyException. "May not declare inputs for a spout"))
      )))

(defn validate-structure! [^StormTopology topology]
  ;; TODO: validate that all subscriptions are to valid component/streams
  )

(defn all-bolts [^StormTopology topology]
  (merge {}  (.get_bolts topology) (.get_transactional_bolts topology)))

(defn all-tuple-spouts [^StormTopology topology]
  (merge {}  (.get_spouts topology) (.get_transactional_spouts topology)))

(defn all-components [^StormTopology topology]
  (apply merge {}
         (for [f thrift/STORM-TOPOLOGY-FIELDS]
           (.getFieldValue topology f)
           )))

(defn acker-inputs [^StormTopology topology]
  (let [bolt-ids (.. topology get_bolts keySet)
        spout-ids (.. topology get_spouts keySet)
        spout-inputs (apply merge
                            (for [id spout-ids]
                              {[id ACKER-INIT-STREAM-ID] ["id"]
                               [id ACKER-ACK-STREAM-ID] ["id"]}
                              ))
        bolt-inputs (apply merge
                           (for [id bolt-ids]
                             {[id ACKER-ACK-STREAM-ID] ["id"]
                              [id ACKER-FAIL-STREAM-ID] ["id"]}
                             ))]
    (merge spout-inputs bolt-inputs)))

(defn add-acker! [num-tasks ^StormTopology ret]
  (let [acker-bolt (thrift/mk-bolt-spec* (acker-inputs ret)
                                         (new backtype.storm.daemon.acker)
                                         {ACKER-ACK-STREAM-ID (thrift/direct-output-fields ["id"])
                                          ACKER-FAIL-STREAM-ID (thrift/direct-output-fields ["id"])
                                          }
                                         :p num-tasks)]
    (dofor [[_ bolt] (all-bolts ret)
            :let [common (.get_common bolt)]]
           (do
             (.put_to_streams common ACKER-ACK-STREAM-ID (thrift/output-fields ["id" "ack-val"]))
             (.put_to_streams common ACKER-FAIL-STREAM-ID (thrift/output-fields ["id"]))
             ))
    (dofor [[_ spout] (all-tuple-spouts ret)
            :let [common (.get_common spout)]]
      (do
        (.put_to_streams common ACKER-INIT-STREAM-ID (thrift/output-fields ["id" "spout-task"]))
        (.put_to_streams common ACKER-ACK-STREAM-ID (thrift/output-fields ["id" "ack-val"]))
        (.put_to_inputs common
                        (GlobalStreamId. ACKER-COMPONENT-ID ACKER-ACK-STREAM-ID)
                        (thrift/mk-direct-grouping))
        (.put_to_inputs common
                        (GlobalStreamId. ACKER-COMPONENT-ID ACKER-FAIL-STREAM-ID)
                        (thrift/mk-direct-grouping))
        ))
    (.put_to_bolts ret "__acker" acker-bolt)
    ))

(defn add-system-streams! [^StormTopology topology]
  (doseq [[_ component] (all-components topology)
          :let [common (.get_common component)]]
    (.put_to_streams common SYSTEM-STREAM-ID (thrift/output-fields ["event"]))
    ;; TODO: consider adding a stats stream for stats aggregation
    ))

(defn system-topology! [storm-conf ^StormTopology topology]
  (validate-basic! topology)
  (let [ret (.deepCopy topology)]
    (add-acker! (storm-conf TOPOLOGY-ACKERS) ret)
    (add-system-streams! ret)
    ;; TODO: need to set up streams/inputs for transactional spout
    ;; TODO: need to set up streams/inputs for transactional bolts
    (validate-structure! ret)
    ret
    ))
