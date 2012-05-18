(ns backtype.storm.daemon.common
  (:use [backtype.storm log config util])
  (:import [backtype.storm.generated StormTopology
            InvalidTopologyException GlobalStreamId])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm Constants])
  (:require [clojure.set :as set])  
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

(defrecord StormBase [storm-name launch-time-secs status num-workers component->executors])

(defrecord SupervisorInfo [time-secs hostname meta scheduler-meta all-ports uptime-secs])

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
      (try-cause
        (apply exec-fn# args#)
      (catch InterruptedException e#
        (throw e#))
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

(defn all-components [^StormTopology topology]
  (apply merge {}
         (for [f thrift/STORM-TOPOLOGY-FIELDS]
           (.getFieldValue topology f)
           )))

(defn validate-structure! [^StormTopology topology]
  ;; validate all the component subscribe from component+stream which actually exists in the topology
  ;; and if it is a fields grouping, validate the corresponding field exists  
  (let [all-components (all-components topology)]
    (doseq [[id comp] all-components
            :let [inputs (.. comp get_common get_inputs)]]
      (doseq [[global-stream-id grouping] inputs
              :let [source-component-id (.get_componentId global-stream-id)
                    source-stream-id    (.get_streamId global-stream-id)]]
        (if-not (contains? all-components source-component-id)
          (throw (InvalidTopologyException. (str "Component: [" id "] subscribes from non-existent component [" source-component-id "]")))
          (let [source-streams (-> all-components (get source-component-id) .get_common .get_streams)]
            (if-not (contains? source-streams source-stream-id)
              (throw (InvalidTopologyException. (str "Component: [" id "] subscribes from non-existent stream: [" source-stream-id "] of component [" source-component-id "]")))
              (if (= :fields (thrift/grouping-type grouping))
                (let [grouping-fields (set (.get_fields grouping))
                      source-stream-fields (-> source-streams (get source-stream-id) .get_output_fields set)
                      diff-fields (set/difference grouping-fields source-stream-fields)]
                  (when-not (empty? diff-fields)
                    (throw (InvalidTopologyException. (str "Component: [" id "] subscribes from stream: [" source-stream-id "] of component [" source-component-id "] with non-existent fields: " diff-fields)))))))))))))

(defn acker-inputs [^StormTopology topology]
  (let [bolt-ids (.. topology get_bolts keySet)
        spout-ids (.. topology get_spouts keySet)
        spout-inputs (apply merge
                            (for [id spout-ids]
                              {[id ACKER-INIT-STREAM-ID] ["id"]}
                              ))
        bolt-inputs (apply merge
                           (for [id bolt-ids]
                             {[id ACKER-ACK-STREAM-ID] ["id"]
                              [id ACKER-FAIL-STREAM-ID] ["id"]}
                             ))]
    (merge spout-inputs bolt-inputs)))

(defn add-acker! [num-executors num-tasks ^StormTopology ret]
  (let [acker-bolt (thrift/mk-bolt-spec* (acker-inputs ret)
                                         (new backtype.storm.daemon.acker)
                                         {ACKER-ACK-STREAM-ID (thrift/direct-output-fields ["id"])
                                          ACKER-FAIL-STREAM-ID (thrift/direct-output-fields ["id"])
                                          }
                                         :p num-executors
                                         :conf {TOPOLOGY-TASKS num-tasks})]
    (dofor [[_ bolt] (.get_bolts ret)
            :let [common (.get_common bolt)]]
           (do
             (.put_to_streams common ACKER-ACK-STREAM-ID (thrift/output-fields ["id" "ack-val"]))
             (.put_to_streams common ACKER-FAIL-STREAM-ID (thrift/output-fields ["id"]))
             ))
    (dofor [[_ spout] (.get_spouts ret)
            :let [common (.get_common spout)]]
      (do
        (.put_to_streams common ACKER-INIT-STREAM-ID (thrift/output-fields ["id" "init-val" "spout-task"]))
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
    (add-acker! (storm-conf TOPOLOGY-ACKER-EXECUTORS) (storm-conf TOPOLOGY-ACKER-TASKS) ret)
    (add-system-streams! ret)
    (validate-structure! ret)
    ret
    ))

(defn has-ackers? [storm-conf]
  (let [tasks (storm-conf TOPOLOGY-ACKER-TASKS)]
    (and (or (nil? tasks) (> tasks 0))
         (> (storm-conf TOPOLOGY-ACKER-EXECUTORS) 0))))

(defn component-conf [storm-conf component]
  (->> component
      .get_common
      .get_json_conf
      from-json
      (merge storm-conf)))

(defn num-start-executors [component]
  (thrift/parallelism-hint (.get_common component)))

(defn- component-parallelism [storm-conf component]
  (let [storm-conf (component-conf storm-conf component)
        num-tasks (or (storm-conf TOPOLOGY-TASKS) (num-start-executors component))
        max-parallelism (storm-conf TOPOLOGY-MAX-TASK-PARALLELISM)
        ]
    (if max-parallelism
      (min max-parallelism num-tasks)
      num-tasks)))

(defn storm-task-info
  "Returns map from task -> component id"
  [^StormTopology user-topology storm-conf]
  (->> (system-topology! storm-conf user-topology)
       all-components
       (map-val (partial component-parallelism storm-conf))
       (sort-by first)
       (mapcat (fn [[c num-tasks]] (repeat num-tasks c)))
       (map (fn [id comp] [id comp]) (iterate (comp int inc) (int 1)))
       (into {})
       ))
