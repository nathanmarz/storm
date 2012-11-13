(ns backtype.storm.daemon.common
  (:use [backtype.storm log config util])
  (:import [backtype.storm.generated StormTopology
            InvalidTopologyException GlobalStreamId])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm.task WorkerTopologyContext])
  (:import [backtype.storm Constants])
  (:import [backtype.storm.spout NoOpSpout])
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

(def SYSTEM-COMPONENT-ID Constants/SYSTEM_COMPONENT_ID)
(def SYSTEM-TICK-STREAM-ID Constants/SYSTEM_TICK_STREAM_ID)
(def METRICS-STREAM-ID Constants/METRICS_STREAM_ID)
(def METRICS-TICK-STREAM-ID Constants/METRICS_TICK_STREAM_ID)

;; the task id is the virtual port
;; node->host is here so that tasks know who to talk to just from assignment
;; this avoid situation where node goes down and task doesn't know what to do information-wise
(defrecord Assignment [master-code-dir node->host executor->node+port executor->start-time-secs])


;; component->executors is a map from spout/bolt id to number of executors for that component
(defrecord StormBase [storm-name launch-time-secs status num-workers component->executors])

(defrecord SupervisorInfo [time-secs hostname assignment-id used-ports meta scheduler-meta uptime-secs])

(defprotocol DaemonCommon
  (waiting? [this]))

(def LS-WORKER-HEARTBEAT "worker-heartbeat")

;; LocalState constants
(def LS-ID "supervisor-id")
(def LS-LOCAL-ASSIGNMENTS "local-assignments")
(def LS-APPROVED-WORKERS "approved-workers")



(defrecord WorkerHeartbeat [time-secs storm-id executors port])

(defrecord ExecutorStats [^long processed
                          ^long acked
                          ^long emitted
                          ^long transferred
                          ^long failed])

(defn new-executor-stats []
  (ExecutorStats. 0 0 0 0 0))

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

(defn all-components [^StormTopology topology]
  (apply merge {}
         (for [f thrift/STORM-TOPOLOGY-FIELDS]
           (.getFieldValue topology f)
           )))

(defn component-conf [component]
  (->> component
      .get_common
      .get_json_conf
      from-json))

(defn validate-basic! [^StormTopology topology]
  (validate-ids! topology)
  (doseq [f thrift/SPOUT-FIELDS
          obj (->> f (.getFieldValue topology) vals)]
    (if-not (empty? (-> obj .get_common .get_inputs))
      (throw (InvalidTopologyException. "May not declare inputs for a spout"))))
  (doseq [[comp-id comp] (all-components topology)
          :let [conf (component-conf comp)
                p (-> comp .get_common thrift/parallelism-hint)]]
    (when (and (> (conf TOPOLOGY-TASKS) 0)
               p
               (<= p 0))
      (throw (InvalidTopologyException. "Number of executors must be greater than 0 when number of tasks is greater than 0"))
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

(defn add-acker! [storm-conf ^StormTopology ret]
  (let [num-executors (storm-conf TOPOLOGY-ACKER-EXECUTORS)
        acker-bolt (thrift/mk-bolt-spec* (acker-inputs ret)
                                         (new backtype.storm.daemon.acker)
                                         {ACKER-ACK-STREAM-ID (thrift/direct-output-fields ["id"])
                                          ACKER-FAIL-STREAM-ID (thrift/direct-output-fields ["id"])
                                          }
                                         :p num-executors
                                         :conf {TOPOLOGY-TASKS num-executors
                                                TOPOLOGY-TICK-TUPLE-FREQ-SECS (storm-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS)})]
    (dofor [[_ bolt] (.get_bolts ret)
            :let [common (.get_common bolt)]]
           (do
             (.put_to_streams common ACKER-ACK-STREAM-ID (thrift/output-fields ["id" "ack-val"]))
             (.put_to_streams common ACKER-FAIL-STREAM-ID (thrift/output-fields ["id"]))
             ))
    (dofor [[_ spout] (.get_spouts ret)
            :let [common (.get_common spout)
                  spout-conf (merge
                               (component-conf spout)
                               {TOPOLOGY-TICK-TUPLE-FREQ-SECS (storm-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS)})]]
      (do
        ;; this set up tick tuples to cause timeouts to be triggered
        (.set_json_conf common (to-json spout-conf))
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

(defn add-metric-streams! [^StormTopology topology]
  (doseq [[_ component] (all-components topology)
          :let [common (.get_common component)]]
    (.put_to_streams common METRICS-STREAM-ID
                     (thrift/output-fields ["task-info" "data-points"]))))

(defn add-system-streams! [^StormTopology topology]
  (doseq [[_ component] (all-components topology)
          :let [common (.get_common component)]]
    (.put_to_streams common SYSTEM-STREAM-ID (thrift/output-fields ["event"]))))


(defn map-occurrences [afn coll]
  (->> coll
       (reduce (fn [[counts new-coll] x]
                 (let [occurs (inc (get counts x 0))]
                   [(assoc counts x occurs) (cons (afn x occurs) new-coll)]))
               [{} []])
       (second)
       (reverse)))

(defn number-duplicates [coll]
  "(number-duplicates [\"a\", \"b\", \"a\"]) => [\"a\", \"b\", \"a#2\"]"
  (map-occurrences (fn [x occurences] (if (>= occurences 2) (str x "#" occurences) x)) coll))

(defn metrics-consumer-register-ids [storm-conf]
  "Generates a list of component ids for each metrics consumer
   e.g. [\"__metrics_org.mycompany.MyMetricsConsumer\", ..] "
  (->> (get storm-conf TOPOLOGY-METRICS-CONSUMER-REGISTER)         
       (map #(get % "class"))
       (number-duplicates)
       (map #(str Constants/METRICS_COMPONENT_ID_PREFIX %))))

(defn metrics-consumer-bolt-specs [components-ids-that-emit-metrics storm-conf]
  (let [inputs (->> (for [comp-id components-ids-that-emit-metrics]
                      {[comp-id METRICS-STREAM-ID] :shuffle})
                    (into {}))
        
        mk-bolt-spec (fn [class arg p]
                       (thrift/mk-bolt-spec*
                        inputs
                        (backtype.storm.metric.MetricsConsumerBolt. class arg)
                        {} :p p :conf {TOPOLOGY-TASKS p}))]
    
    (map
     (fn [component-id register]           
       [component-id (mk-bolt-spec (get register "class")
                                   (get register "argument")
                                   (or (get register "parallelism.hint") 1))])
     
     (metrics-consumer-register-ids storm-conf)
     (get storm-conf TOPOLOGY-METRICS-CONSUMER-REGISTER))))

(defn add-metric-components! [storm-conf ^StormTopology topology]
  (doseq [[comp-id bolt-spec] (metrics-consumer-bolt-specs (keys (all-components topology)) storm-conf)]
    (.put_to_bolts topology comp-id bolt-spec)))

(defn add-system-components! [^StormTopology topology]
  (let [system-spout (thrift/mk-spout-spec*
                      (NoOpSpout.)
                      {SYSTEM-TICK-STREAM-ID (thrift/output-fields ["rate_secs"])
                       METRICS-TICK-STREAM-ID (thrift/output-fields ["interval"])}
                      :p 0
                      :conf {TOPOLOGY-TASKS 0})]
    (.put_to_spouts topology SYSTEM-COMPONENT-ID system-spout)))

(defn system-topology! [storm-conf ^StormTopology topology]
  (validate-basic! topology)
  (let [ret (.deepCopy topology)]
    (add-acker! storm-conf ret)
    (add-metric-components! storm-conf ret)
    (add-metric-streams! ret)    
    (add-system-streams! ret)
    (add-system-components! ret)
    (validate-structure! ret)
    ret
    ))

(defn has-ackers? [storm-conf]
  (> (storm-conf TOPOLOGY-ACKER-EXECUTORS) 0))

(defn num-start-executors [component]
  (thrift/parallelism-hint (.get_common component)))

(defn storm-task-info
  "Returns map from task -> component id"
  [^StormTopology user-topology storm-conf]
  (->> (system-topology! storm-conf user-topology)
       all-components
       (map-val (comp #(get % TOPOLOGY-TASKS) component-conf))
       (sort-by first)
       (mapcat (fn [[c num-tasks]] (repeat num-tasks c)))
       (map (fn [id comp] [id comp]) (iterate (comp int inc) (int 1)))
       (into {})
       ))

(defn executor-id->tasks [[first-task-id last-task-id]]
  (->> (range first-task-id (inc last-task-id))
       (map int)))

(defn worker-context [worker]
  (WorkerTopologyContext. (:system-topology worker)
                          (:storm-conf worker)
                          (:task->component worker)
                          (:component->sorted-tasks worker)
                          (:component->stream->fields worker)
                          (:storm-id worker)
                          (supervisor-storm-resources-path
                            (supervisor-stormdist-root (:conf worker) (:storm-id worker)))
                          (worker-pids-root (:conf worker) (:worker-id worker))
                          (:port worker)
                          (:task-ids worker)
                          (:default-shared-resources worker)
                          (:user-shared-resources worker)
                          ))


(defn to-task->node+port [executor->node+port]
  (->> executor->node+port
       (mapcat (fn [[e node+port]] (for [t (executor-id->tasks e)] [t node+port])))
       (into {})))
