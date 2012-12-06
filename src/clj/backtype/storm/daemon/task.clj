(ns backtype.storm.daemon.task
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap])
  (:import [backtype.storm.hooks ITaskHook])
  (:import [backtype.storm.tuple Tuple])
  (:import [backtype.storm.generated SpoutSpec Bolt StateSpoutSpec])
  (:import [backtype.storm.hooks.info SpoutAckInfo SpoutFailInfo
            EmitInfo BoltFailInfo BoltAckInfo])
  (:require [backtype.storm [tuple :as tuple]])
  (:require [backtype.storm.daemon.builtin-metrics :as builtin-metrics]))

(bootstrap)

(defn mk-topology-context-builder [worker executor-data topology]
  (let [conf (:conf worker)]
    #(TopologyContext.
      topology
      (:storm-conf worker)
      (:task->component worker)
      (:component->sorted-tasks worker)
      (:component->stream->fields worker)
      (:storm-id worker)
      (supervisor-storm-resources-path
        (supervisor-stormdist-root conf (:storm-id worker)))
      (worker-pids-root conf (:worker-id worker))
      (int %)
      (:port worker)
      (:task-ids worker)
      (:default-shared-resources worker)
      (:user-shared-resources worker)
      (:shared-executor-data executor-data)
      (:interval->task->metric-registry executor-data)
      (:open-or-prepare-was-called? executor-data))))

(defn system-topology-context [worker executor-data tid]
  ((mk-topology-context-builder
    worker
    executor-data
    (:system-topology worker))
   tid))

(defn user-topology-context [worker executor-data tid]
  ((mk-topology-context-builder
    worker
    executor-data
    (:topology worker))
   tid))

(defn- get-task-object [^TopologyContext topology component-id]
  (let [spouts (.get_spouts topology)
        bolts (.get_bolts topology)
        state-spouts (.get_state_spouts topology)
        obj (Utils/getSetComponentObject
             (cond
              (contains? spouts component-id) (.get_spout_object ^SpoutSpec (get spouts component-id))
              (contains? bolts component-id) (.get_bolt_object ^Bolt (get bolts component-id))
              (contains? state-spouts component-id) (.get_state_spout_object ^StateSpoutSpec (get state-spouts component-id))
              true (throw-runtime "Could not find " component-id " in " topology)))
        obj (if (instance? ShellComponent obj)
              (if (contains? spouts component-id)
                (ShellSpout. obj)
                (ShellBolt. obj))
              obj )
        obj (if (instance? JavaObject obj)
              (thrift/instantiate-java-object obj)
              obj )]
    obj
    ))

(defn get-context-hooks [^TopologyContext context]
  (.getHooks context))

(defn hooks-empty? [^Collection hooks]
  (.isEmpty hooks))

(defmacro apply-hooks [topology-context method-sym info-form]
  (let [hook-sym (with-meta (gensym "hook") {:tag 'backtype.storm.hooks.ITaskHook})]
    `(let [hooks# (get-context-hooks ~topology-context)]
       (when-not (hooks-empty? hooks#)
         (let [info# ~info-form]
           (fast-list-iter [~hook-sym hooks#]
             (~method-sym ~hook-sym info#)
             ))))))


;; TODO: this is all expensive... should be precomputed
(defn send-unanchored
  ([task-data stream values overflow-buffer]
    (let [^TopologyContext topology-context (:system-context task-data)
          tasks-fn (:tasks-fn task-data)
          transfer-fn (-> task-data :executor-data :transfer-fn)
          out-tuple (TupleImpl. topology-context
                                 values
                                 (.getThisTaskId topology-context)
                                 stream)]
      (fast-list-iter [t (tasks-fn stream values)]
        (transfer-fn t
                     out-tuple
                     overflow-buffer)
        )))
    ([task-data stream values]
      (send-unanchored task-data stream values nil)
      ))

(defn mk-tasks-fn [task-data]
  (let [task-id (:task-id task-data)
        executor-data (:executor-data task-data)
        component-id (:component-id executor-data)
        ^WorkerTopologyContext worker-context (:worker-context executor-data)
        storm-conf (:storm-conf executor-data)
        emit-sampler (mk-stats-sampler storm-conf)
        stream->component->grouper (:stream->component->grouper executor-data)
        user-context (:user-context task-data)
        executor-stats (:stats executor-data)
        debug? (= true (storm-conf TOPOLOGY-DEBUG))]
        
    (fn ([^Integer out-task-id ^String stream ^List values]
          (when debug?
            (log-message "Emitting direct: " out-task-id "; " component-id " " stream " " values))
          (let [target-component (.getComponentId worker-context out-task-id)
                component->grouping (get stream->component->grouper stream)
                grouping (get component->grouping target-component)
                out-task-id (if grouping out-task-id)]
            (when (and (not-nil? grouping) (not= :direct grouping))
              (throw (IllegalArgumentException. "Cannot emitDirect to a task expecting a regular grouping")))                          
            (apply-hooks user-context .emit (EmitInfo. values stream task-id [out-task-id]))
            (when (emit-sampler)
              (builtin-metrics/emitted-tuple! (:builtin-metrics task-data) executor-stats stream)
              (stats/emitted-tuple! executor-stats stream)
              (if out-task-id
                (stats/transferred-tuples! executor-stats stream 1)
                (builtin-metrics/transferred-tuple! (:builtin-metrics task-data) executor-stats stream 1)))
            (if out-task-id [out-task-id])
            ))
        ([^String stream ^List values]
           (when debug?
             (log-message "Emitting: " component-id " " stream " " values))
           (let [out-tasks (ArrayList.)]
             (fast-map-iter [[out-component grouper] (get stream->component->grouper stream)]
               (when (= :direct grouper)
                  ;;  TODO: this is wrong, need to check how the stream was declared
                  (throw (IllegalArgumentException. "Cannot do regular emit to direct stream")))
               (let [comp-tasks (grouper task-id values)]
                 (if (or (sequential? comp-tasks) (instance? Collection comp-tasks))
                   (.addAll out-tasks comp-tasks)
                   (.add out-tasks comp-tasks)
                   )))
             (apply-hooks user-context .emit (EmitInfo. values stream task-id out-tasks))
             (when (emit-sampler)
               (stats/emitted-tuple! executor-stats stream)
               (builtin-metrics/emitted-tuple! (:builtin-metrics task-data) executor-stats stream)              
               (stats/transferred-tuples! executor-stats stream (count out-tasks))
               (builtin-metrics/transferred-tuple! (:builtin-metrics task-data) executor-stats stream (count out-tasks)))
             out-tasks)))
    ))

(defn mk-task-data [executor-data task-id]
  (recursive-map
    :executor-data executor-data
    :task-id task-id
    :system-context (system-topology-context (:worker executor-data) executor-data task-id)
    :user-context (user-topology-context (:worker executor-data) executor-data task-id)
    :builtin-metrics (builtin-metrics/make-data (:type executor-data))
    :tasks-fn (mk-tasks-fn <>)
    :object (get-task-object (.getRawTopology ^TopologyContext (:system-context <>)) (:component-id executor-data))))


(defn mk-task [executor-data task-id]
  (let [task-data (mk-task-data executor-data task-id)
        storm-conf (:storm-conf executor-data)]
    (doseq [klass (storm-conf TOPOLOGY-AUTO-TASK-HOOKS)]
      (.addTaskHook ^TopologyContext (:user-context task-data) (-> klass Class/forName .newInstance)))
    ;; when this is called, the threads for the executor haven't been started yet,
    ;; so we won't be risking trampling on the single-threaded claim strategy disruptor queue
    (send-unanchored task-data SYSTEM-STREAM-ID ["startup"])
    task-data
    ))
