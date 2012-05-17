(ns backtype.storm.daemon.executor
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap])
  (:import [java.util.concurrent ConcurrentLinkedQueue ConcurrentHashMap LinkedBlockingQueue])
  (:import [backtype.storm.hooks ITaskHook])
  (:import [backtype.storm.tuple Tuple])
  (:import [backtype.storm.hooks.info SpoutAckInfo SpoutFailInfo
              EmitInfo BoltFailInfo BoltAckInfo])
  (:require [backtype.storm [tuple :as tuple]]))

(bootstrap)

(defn- mk-fields-grouper [^Fields out-fields ^Fields group-fields ^List target-tasks]
  (let [num-tasks (count target-tasks)
        task-getter (fn [i] (.get target-tasks i))]
    (fn [^List values]
      (-> (.select out-fields group-fields values)
          tuple/list-hash-code
          (mod num-tasks)
          task-getter))))

(defn- mk-shuffle-grouper [^List target-tasks]
  (let [num-tasks (count target-tasks)
        choices (rotating-random-range num-tasks)]
    (fn [tuple]
      (->> (acquire-random-range-id choices num-tasks)
           (.get target-tasks)))))

(defn- mk-custom-grouper [^CustomStreamGrouping grouping ^TopologyContext context ^Fields out-fields target-tasks]
  (.prepare grouping context out-fields target-tasks)
  (fn [^List values]
    (.chooseTasks grouping values)
    ))

(defn- mk-grouper
  "Returns a function that returns a vector of which task indices to send tuple to, or just a single task index."
  [^TopologyContext context ^Fields out-fields thrift-grouping ^List target-tasks]
  (let [num-tasks (count target-tasks)
        random (Random.)
        target-tasks (vec (sort target-tasks))]
    (condp = (thrift/grouping-type thrift-grouping)
      :fields
        (if (thrift/global-grouping? thrift-grouping)
          (fn [tuple]
            ;; It's possible for target to have multiple tasks if it reads multiple sources
            (first target-tasks))
          (let [group-fields (Fields. (thrift/field-grouping thrift-grouping))]
            (mk-fields-grouper out-fields group-fields target-tasks)
            ))
      :all
        (fn [tuple] target-tasks)
      :shuffle
        (mk-shuffle-grouper target-tasks)
      :local-or-shuffle
        (let [same-tasks (set/intersection
                           (set target-tasks)
                           (set (.getThisWorkerTasks context)))]
          (if-not (empty? same-tasks)
            (mk-shuffle-grouper (vec same-tasks))
            (mk-shuffle-grouper target-tasks)))
      :none
        (fn [tuple]
          (let [i (mod (.nextInt random) num-tasks)]
            (.get target-tasks i)
            ))
      :custom-object
        (let [grouping (thrift/instantiate-java-object (.get_custom_object thrift-grouping))]
          (mk-custom-grouper grouping context out-fields target-tasks))
      :custom-serialized
        (let [grouping (Utils/deserialize (.get_custom_serialized thrift-grouping))]
          (mk-custom-grouper grouping context out-fields target-tasks))
      :direct
        :direct
      )))


(defn- get-task-object [topology component-id]
  (let [spouts (.get_spouts topology)
        bolts (.get_bolts topology)
        state-spouts (.get_state_spouts topology)
        obj (Utils/getSetComponentObject
             (cond
              (contains? spouts component-id) (.get_spout_object (get spouts component-id))
              (contains? bolts component-id) (.get_bolt_object (get bolts component-id))
              (contains? state-spouts component-id) (.get_state_spout_object (get state-spouts component-id))
              true (throw (RuntimeException. (str "Could not find " component-id " in " topology)))))
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

(defn- get-context-hooks [^TopologyContext context]
  (.getHooks context))

(defmacro apply-hooks [topology-context method-sym info-form]
  (let [hook-sym (with-meta (gensym "hook") {:tag 'backtype.storm.hooks.ITaskHook})]
    `(let [hooks# (get-context-hooks ~topology-context)]
       (when-not (empty? hooks#)
         (let [info# ~info-form]
           (doseq [~hook-sym hooks#]
             (~method-sym ~hook-sym info#)
             ))))))

(defn outbound-components
  "Returns map of stream id to component id to grouper"
  [^TopologyContext topology-context ^TopologyContext user-context]
  (let [output-groupings (clojurify-structure (.getThisTargets topology-context))]
     (into {}
       (for [[stream-id component->grouping] output-groupings
             :let [out-fields (.getThisOutputFields topology-context stream-id)
                   component->grouping (filter-key #(pos? (count (.getComponentTasks topology-context %)))
                                       component->grouping)]]         
         [stream-id
          (into {}
                (for [[component tgrouping] component->grouping]
                  [component (mk-grouper user-context
                                         out-fields
                                         tgrouping
                                         (.getComponentTasks topology-context component)
                                         )]
                  ))]))
    ))



(defmulti mk-executors class-selector)
(defmulti close-component class-selector)
(defmulti mk-task-stats class-selector)

(defn- get-readable-name [topology-context]
  (.getThisComponentId topology-context))

(defn- normalized-component-conf [storm-conf topology-context component-id]
  (let [to-remove (disj (set ALL-CONFIGS)
                        TOPOLOGY-DEBUG
                        TOPOLOGY-MAX-SPOUT-PENDING
                        TOPOLOGY-MAX-TASK-PARALLELISM
                        TOPOLOGY-TRANSACTIONAL-ID)
        spec-conf (-> topology-context
                      (.getComponentCommon component-id)
                      .get_json_conf
                      from-json)]
    (merge storm-conf (apply dissoc spec-conf to-remove))
    ))

(defn send-unanchored [^TopologyContext topology-context tasks-fn transfer-fn stream values]
  (doseq [t (tasks-fn stream values)]
    (transfer-fn t
                 (Tuple. topology-context
                         values
                         (.getThisTaskId topology-context)
                         stream))))

(defn mk-executor [worker ^TopologyContext topology-context ^TopologyContext user-context]
  (let [task-id (.getThisTaskId topology-context)

        ;; TODO refactor...
        conf (:conf worker)
        storm-conf (:storm-conf worker)
        storm-id (:storm-id worker)
        cluster-state (:cluster-state worker)
        storm-active-atom (:storm-active-atom worker)
        transfer-fn (:transfer-fn worker)
        suicide-fn (:suicide-fn worker)
        receive-queue ((:receive-queue-map worker) task-id)
    
    
    
    
    
    
        worker-port (.getThisWorkerPort topology-context)
        component-id (.getThisComponentId topology-context)
        storm-conf (normalized-component-conf storm-conf topology-context component-id)
        _ (log-message "Loading task " component-id ":" task-id)
        task-info (.getTaskToComponent topology-context)
        active (atom true)
        uptime (uptime-computer)
        storm-cluster-state (cluster/mk-storm-cluster-state cluster-state)
        
        task-object (get-task-object (.getRawTopology topology-context)
                                     (.getThisComponentId topology-context))
        task-stats (mk-task-stats task-object (sampling-rate storm-conf))

        report-error (fn [error]
                       (log-error error)
                       (cluster/report-error storm-cluster-state storm-id component-id error))
        
        report-error-and-die (fn [error]
                               (report-error error)
                               (apply-hooks user-context .error error)
                               (suicide-fn))

        ;; heartbeat ASAP so nimbus doesn't reassign
        heartbeat-thread (async-loop
                          (fn []
                            (swap! (:taskbeats worker) assoc task-id
                                              (TaskHeartbeat. (current-time-secs)
                                                              (uptime)
                                                              (stats/render-stats! task-stats)))
                            (when @active (storm-conf TASK-HEARTBEAT-FREQUENCY-SECS))
                            )
                          :priority Thread/MAX_PRIORITY
                          :kill-fn report-error-and-die)

        _ (doseq [klass (storm-conf TOPOLOGY-AUTO-TASK-HOOKS)]
            (.addTaskHook user-context (-> klass Class/forName .newInstance)))

        stream->component->grouper (outbound-components topology-context user-context)
        component->tasks (reverse-map task-info)
        
        ;; TODO: consider DRYing things up and moving stats        
        task-readable-name (get-readable-name topology-context)

        emit-sampler (mk-stats-sampler storm-conf)
        tasks-fn (fn ([^Integer out-task-id ^String stream ^List values]
                        (when (= true (storm-conf TOPOLOGY-DEBUG))
                          (log-message "Emitting direct: " out-task-id "; " task-readable-name " " stream " " values))
                        (let [target-component (.getComponentId topology-context out-task-id)
                              component->grouping (stream->component->grouper stream)
                              grouping (get component->grouping target-component)
                              out-task-id (if grouping out-task-id)]
                          (when (and (not-nil? grouping) (not= :direct grouping))
                            (throw (IllegalArgumentException. "Cannot emitDirect to a task expecting a regular grouping")))                          
                          (apply-hooks user-context .emit (EmitInfo. values stream [out-task-id]))
                          (when (emit-sampler)
                            (stats/emitted-tuple! task-stats stream)
                            (if out-task-id
                              (stats/transferred-tuples! task-stats stream 1)))
                          (if out-task-id [out-task-id])
                          ))
                   ([^String stream ^List values]
                      (when (= true (storm-conf TOPOLOGY-DEBUG))
                        (log-message "Emitting: " task-readable-name " " stream " " values))
                      (let [;; TODO: this doesn't seem to be very fast
                            ;; and seems to be the current bottleneck
                            out-tasks (mapcat
                                       (fn [[out-component grouper]]
                                         (when (= :direct grouper)
                                           ;;  TODO: this is wrong, need to check how the stream was declared
                                           (throw (IllegalArgumentException. "Cannot do regular emit to direct stream")))
                                         (collectify (grouper values)))
                                       (stream->component->grouper stream))]
                        (apply-hooks user-context .emit (EmitInfo. values stream out-tasks))
                        (when (emit-sampler)
                          (stats/emitted-tuple! task-stats stream)
                          (stats/transferred-tuples! task-stats stream (count out-tasks)))
                        out-tasks)))
        _ (send-unanchored topology-context tasks-fn transfer-fn SYSTEM-STREAM-ID ["startup"])
        executor-threads (dofor
                          [exec (with-error-reaction report-error-and-die
                                  (mk-executors task-object storm-conf receive-queue tasks-fn
                                                transfer-fn
                                                storm-active-atom topology-context
                                                user-context task-stats report-error))]
                          (async-loop (fn [] (exec) (when @active 0))
                                      :kill-fn report-error-and-die))
        system-threads [heartbeat-thread]
        all-threads  (concat executor-threads system-threads)]
    (log-message "Finished loading task " component-id ":" task-id)
    ;; TODO: add method here to get rendered stats... have worker call that when heartbeating
    (reify
      Shutdownable
      (shutdown
        [this]
        (log-message "Shutting down task " storm-id ":" task-id)
        (reset! active false)
        ;; put an empty message into receive-queue
        ;; empty messages are skip messages (this unblocks the receive-queue.take thread)
        (.put receive-queue (byte-array []))
        (doseq [t all-threads]
          (.interrupt t)
          (.join t))
        (doseq [hook (.getHooks user-context)]
          (.cleanup hook))
        ;; remove its taskbeats from worker if exists
        (swap! (:taskbeats worker) dissoc task-id)
        (.disconnect storm-cluster-state)
        (close-component task-object)
        (log-message "Shut down task " storm-id ":" task-id))
      DaemonCommon
      (waiting? [this]
        ;; executor threads are independent since they don't sleep
        ;; -> they block on zeromq
        (every? (memfn sleeping?) system-threads)
        ))))

(defn- fail-spout-msg [^ISpout spout ^TopologyContext user-context storm-conf msg-id tuple-info time-delta task-stats sampler]
  (log-message "Failing message " msg-id ": " tuple-info)
  (.fail spout msg-id)
  (apply-hooks user-context .spoutFail (SpoutFailInfo. msg-id time-delta))
  (when (sampler)
    (stats/spout-failed-tuple! task-stats (:stream tuple-info) time-delta)
    ))

(defn- ack-spout-msg [^ISpout spout ^TopologyContext user-context storm-conf msg-id tuple-info time-delta task-stats sampler]
  (when (= true (storm-conf TOPOLOGY-DEBUG))
    (log-message "Acking message " msg-id))
  (.ack spout msg-id)
  (apply-hooks user-context .spoutAck (SpoutAckInfo. msg-id time-delta))
  (when (sampler)
    (stats/spout-acked-tuple! task-stats (:stream tuple-info) time-delta)
    ))

(defn mk-task-receiver [^LinkedBlockingQueue receive-queue ^KryoTupleDeserializer deserializer tuple-action-fn]
  (fn []
    (let [msg (.take receive-queue)
          is-tuple? (instance? Tuple msg)]
      (when (or is-tuple? (not (empty? msg))) ; skip empty messages (used during shutdown)
        (log-debug "Processing message " msg)
        (let [^Tuple tuple (if is-tuple? msg (.deserialize deserializer msg))]
          (tuple-action-fn tuple)
          ))
      )))

(defmethod mk-executors ISpout [^ISpout spout storm-conf ^LinkedBlockingQueue receive-queue tasks-fn transfer-fn storm-active-atom
                                ^TopologyContext topology-context ^TopologyContext user-context
                                task-stats report-error-fn]
  (let [wait-fn (fn [] @storm-active-atom)
        last-active (atom false)
        task-id (.getThisTaskId topology-context)
        component-id (.getThisComponentId topology-context)
        max-spout-pending (storm-conf TOPOLOGY-MAX-SPOUT-PENDING)
        deserializer (KryoTupleDeserializer. storm-conf topology-context)
        event-queue (ConcurrentLinkedQueue.)
        sampler (mk-stats-sampler storm-conf)
        
        pending (TimeCacheMap.
                 (int (storm-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS))
                 (reify TimeCacheMap$ExpiredCallback
                   (expire [this msg-id [spout-id tuple-info start-time-ms]]
                     (let [time-delta (time-delta-ms start-time-ms)]
                       (.add event-queue #(fail-spout-msg spout user-context storm-conf spout-id tuple-info time-delta task-stats sampler)))
                     )))
        send-spout-msg (fn [out-stream-id values message-id out-task-id]
                         (let [out-tasks (if out-task-id
                                           (tasks-fn out-task-id out-stream-id values)
                                           (tasks-fn out-stream-id values))
                               root-id (MessageId/generateId)
                               rooted? (and message-id (has-ackers? storm-conf))
                               out-ids (dofor [t out-tasks] (MessageId/generateId))
                               out-tuples (dofor [id out-ids]
                                            (let [tuple-id (if rooted?
                                                             (MessageId/makeRootId root-id id)
                                                             (MessageId/makeUnanchored))]
                                              (Tuple. topology-context
                                                      values
                                                      task-id
                                                      out-stream-id
                                                      tuple-id)))]
                           (dorun
                            (map transfer-fn out-tasks out-tuples))
                           (if rooted?
                             (do
                               (.put pending root-id [message-id
                                                      {:stream out-stream-id :values values}
                                                      (System/currentTimeMillis)])
                               (send-unanchored topology-context tasks-fn transfer-fn
                                                ACKER-INIT-STREAM-ID
                                                [root-id (bit-xor-vals out-ids) task-id]))
                             (when message-id
                               (.add event-queue #(ack-spout-msg spout user-context storm-conf message-id {:stream out-stream-id :values values} 0 task-stats sampler))))
                           (or out-tasks [])
                           ))
        output-collector (reify ISpoutOutputCollector
                           (^List emit [this ^String stream-id ^List tuple ^Object message-id]
                             (send-spout-msg stream-id tuple message-id nil)
                             )
                           (^void emitDirect [this ^int out-task-id ^String stream-id
                                              ^List tuple ^Object message-id]
                             (send-spout-msg stream-id tuple message-id out-task-id)
                             )
                           (reportError [this error]
                             (report-error-fn error)
                             ))
        tuple-action-fn (fn [^Tuple tuple]
                          (let [id (.getValue tuple 0)
                                [spout-id tuple-finished-info start-time-ms] (.remove pending id)]
                            (when spout-id
                              (let [time-delta (time-delta-ms start-time-ms)]
                                (condp = (.getSourceStreamId tuple)
                                    ACKER-ACK-STREAM-ID (.add event-queue #(ack-spout-msg spout user-context storm-conf spout-id
                                                                                          tuple-finished-info time-delta task-stats sampler))
                                    ACKER-FAIL-STREAM-ID (.add event-queue #(fail-spout-msg spout user-context storm-conf spout-id
                                                                                            tuple-finished-info time-delta task-stats sampler))
                                    )))
                            ;; TODO: on failure, emit tuple to failure stream
                            ))]
    (log-message "Opening spout " component-id ":" task-id)
    (.open spout storm-conf user-context (SpoutOutputCollector. output-collector))
    (log-message "Opened spout " component-id ":" task-id)
    ;; TODO: should redesign this to only use one thread
    [(fn []
       ;; This design requires that spouts be non-blocking
       (loop []
         (when-let [event (.poll event-queue)]
           (event)
           (recur)
           ))
       (if (or (not max-spout-pending)
               (< (.size pending) max-spout-pending))
         (if-let [active? (wait-fn)]
           (do
             (when-not @last-active
               (reset! last-active true)
               (log-message "Activating spout " component-id ":" task-id)
               (.activate spout))
             (.nextTuple spout))
           (do
             (when @last-active
               (reset! last-active false)
               (log-message "Deactivating spout " component-id ":" task-id)
               (.deactivate spout))
             ;; TODO: log that it's getting throttled
             (Time/sleep 100)))
         ))
         (mk-task-receiver receive-queue deserializer tuple-action-fn)
     ]
    ))

(defn- tuple-time-delta! [^Map start-times ^Tuple tuple]
  (time-delta-ms (.remove start-times tuple)))

(defn put-xor! [^Map pending key id]
  (let [curr (or (.get pending key) (long 0))]
    ;; TODO: this portion is not thread safe (multiple threads updating same value at same time)
    (.put pending key (bit-xor curr id))))

(defmethod mk-executors IBolt [^IBolt bolt storm-conf ^LinkedBlockingQueue receive-queue tasks-fn transfer-fn storm-active-atom
                               ^TopologyContext topology-context ^TopologyContext user-context
                               task-stats report-error-fn]
  (let [deserializer (KryoTupleDeserializer. storm-conf topology-context)
        task-id (.getThisTaskId topology-context)
        component-id (.getThisComponentId topology-context)
        tuple-start-times (ConcurrentHashMap.)
        sampler (mk-stats-sampler storm-conf)
        pending-acks (ConcurrentHashMap.)
        bolt-emit (fn [stream anchors values task]
                    (let [out-tasks (if task
                                      (tasks-fn task stream values)
                                      (tasks-fn stream values))]
                      (doseq [t out-tasks
                              :let [anchors-to-ids (HashMap.)]]
                        (doseq [^Tuple a anchors
                                :let [edge-id (MessageId/generateId)]]
                          (put-xor! pending-acks a edge-id)
                          (doseq [root-id (-> a .getMessageId .getAnchorsToIds .keySet)]
                            (put-xor! anchors-to-ids root-id edge-id)))
                        (transfer-fn t
                                     (Tuple. topology-context
                                             values
                                             task-id
                                             stream
                                             (MessageId/makeId anchors-to-ids))))
                      (or out-tasks [])))
        output-collector (reify IOutputCollector
                           (emit [this stream anchors values]
                             (bolt-emit stream anchors values nil))
                           (emitDirect [this task stream anchors values]
                             (bolt-emit stream anchors values task))
                           (^void ack [this ^Tuple tuple]
                             (let [ack-val (or (.remove pending-acks tuple) (long 0))]
                               (doseq [[root id] (.. tuple getMessageId getAnchorsToIds)]
                                 (send-unanchored topology-context tasks-fn transfer-fn
                                                  ACKER-ACK-STREAM-ID [root (bit-xor id ack-val)])
                                 ))
                             (let [delta (tuple-time-delta! tuple-start-times tuple)]
                               (apply-hooks user-context .boltAck (BoltAckInfo. tuple delta))
                               (when (sampler)
                                 (stats/bolt-acked-tuple! task-stats
                                                          (.getSourceComponent tuple)
                                                          (.getSourceStreamId tuple)
                                                          delta)
                                 )))
                           (^void fail [this ^Tuple tuple]
                             (.remove pending-acks tuple)
                             (doseq [root (.. tuple getMessageId getAnchors)]
                               (send-unanchored topology-context tasks-fn transfer-fn
                                                ACKER-FAIL-STREAM-ID [root]))
                             (let [delta (tuple-time-delta! tuple-start-times tuple)]
                               (apply-hooks user-context .boltFail (BoltFailInfo. tuple delta))
                               (when (sampler)
                                 (stats/bolt-failed-tuple! task-stats
                                                           (.getSourceComponent tuple)
                                                           (.getSourceStreamId tuple)
                                                           delta)
                                 )))
                           (reportError [this error]
                             (report-error-fn error)
                             ))
        tuple-action-fn (fn [^Tuple tuple]
                          ;; synchronization needs to be done with a key provided by this bolt, otherwise:
                          ;; spout 1 sends synchronization (s1), dies, same spout restarts somewhere else, sends synchronization (s2) and incremental update. s2 and update finish before s1 -> lose the incremental update
                          ;; TODO: for state sync, need to first send sync messages in a loop and receive tuples until synchronization
                          ;; buffer other tuples until fully synchronized, then process all of those tuples
                          ;; then go into normal loop
                          ;; spill to disk?
                          ;; could be receiving incremental updates while waiting for sync or even a partial sync because of another failed task
                          ;; should remember sync requests and include a random sync id in the request. drop anything not related to active sync requests
                          ;; or just timeout the sync messages that are coming in until full sync is hit from that task
                          ;; need to drop incremental updates from tasks where waiting for sync. otherwise, buffer the incremental updates
                          ;; TODO: for state sync, need to check if tuple comes from state spout. if so, update state
                          ;; TODO: how to handle incremental updates as well as synchronizations at same time
                          ;; TODO: need to version tuples somehow
 
                          (log-debug "Received tuple " tuple " at task " (.getThisTaskId topology-context))
                          (.put tuple-start-times tuple (System/currentTimeMillis))
             
                          (.execute bolt tuple))]
    (log-message "Preparing bolt " component-id ":" task-id)
    (.prepare bolt
              storm-conf
              user-context
              (OutputCollector. output-collector))
    (log-message "Prepared bolt " component-id ":" task-id)
    ;; TODO: can get any SubscribedState objects out of the context now
    [(mk-task-receiver receive-queue deserializer tuple-action-fn)]
    ))

(defmethod close-component ISpout [spout]
  (.close spout))

(defmethod close-component IBolt [bolt]
  (.cleanup bolt))

(defmethod mk-task-stats ISpout [_ rate]
  (stats/mk-spout-stats rate))

(defmethod mk-task-stats IBolt [_ rate]
  (stats/mk-bolt-stats rate))
