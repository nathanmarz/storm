(ns backtype.storm.daemon.executor
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap])
  (:import [backtype.storm.hooks ITaskHook])
  (:import [backtype.storm.tuple Tuple])
  (:import [backtype.storm.spout ISpoutWaitStrategy])
  (:import [backtype.storm.hooks.info SpoutAckInfo SpoutFailInfo
              EmitInfo BoltFailInfo BoltAckInfo])
  (:require [backtype.storm [tuple :as tuple]])
  (:require [backtype.storm.daemon [task :as task]])
  )

(bootstrap)

(defn- mk-fields-grouper [^Fields out-fields ^Fields group-fields ^List target-tasks]
  (let [num-tasks (count target-tasks)
        task-getter (fn [i] (.get target-tasks i))]
    (fn [task-id ^List values]
      (-> (.select out-fields group-fields values)
          tuple/list-hash-code
          (mod num-tasks)
          task-getter))))

(defn- mk-shuffle-grouper [^List target-tasks]
  (let [choices (rotating-random-range target-tasks)]
    (fn [task-id tuple]
      (acquire-random-range-id choices))))

(defn- mk-custom-grouper [^CustomStreamGrouping grouping ^WorkerTopologyContext context ^String component-id ^String stream-id target-tasks]
  (.prepare grouping context (GlobalStreamId. component-id stream-id) target-tasks)
  (fn [task-id ^List values]
    (.chooseTasks grouping task-id values)
    ))

(defn- mk-grouper
  "Returns a function that returns a vector of which task indices to send tuple to, or just a single task index."
  [^WorkerTopologyContext context component-id stream-id ^Fields out-fields thrift-grouping ^List target-tasks]
  (let [num-tasks (count target-tasks)
        random (Random.)
        target-tasks (vec (sort target-tasks))]
    (condp = (thrift/grouping-type thrift-grouping)
      :fields
        (if (thrift/global-grouping? thrift-grouping)
          (fn [task-id tuple]
            ;; It's possible for target to have multiple tasks if it reads multiple sources
            (first target-tasks))
          (let [group-fields (Fields. (thrift/field-grouping thrift-grouping))]
            (mk-fields-grouper out-fields group-fields target-tasks)
            ))
      :all
        (fn [task-id tuple] target-tasks)
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
        (fn [task-id tuple]
          (let [i (mod (.nextInt random) num-tasks)]
            (.get target-tasks i)
            ))
      :custom-object
        (let [grouping (thrift/instantiate-java-object (.get_custom_object thrift-grouping))]
          (mk-custom-grouper grouping context component-id stream-id target-tasks))
      :custom-serialized
        (let [grouping (Utils/deserialize (.get_custom_serialized thrift-grouping))]
          (mk-custom-grouper grouping context component-id stream-id target-tasks))
      :direct
        :direct
      )))

(defn- outbound-groupings [^WorkerTopologyContext worker-context this-component-id stream-id out-fields component->grouping]
  (->> component->grouping
       (filter-key #(-> worker-context
                        (.getComponentTasks %)
                        count
                        pos?))
       (map (fn [[component tgrouping]]
               [component
                (mk-grouper worker-context
                            this-component-id
                            stream-id
                            out-fields
                            tgrouping
                            (.getComponentTasks worker-context component)
                            )]))
       (into {})
       (HashMap.)))

(defn outbound-components
  "Returns map of stream id to component id to grouper"
  [^WorkerTopologyContext worker-context component-id]
  (->> (.getTargets worker-context component-id)
        clojurify-structure
        (map (fn [[stream-id component->grouping]]
               [stream-id
                (outbound-groupings
                  worker-context
                  component-id
                  stream-id
                  (.getComponentOutputFields worker-context component-id stream-id)
                  component->grouping)]))
         (into {})
         (HashMap.)))

(defn executor-type [^WorkerTopologyContext context component-id]
  (let [topology (.getRawTopology context)
        spouts (.get_spouts topology)
        bolts (.get_bolts topology)
        ]
    (cond (contains? spouts component-id) :spout
          (contains? bolts component-id) :bolt
          :else (throw-runtime "Could not find " component-id " in topology " topology))))

(defn executor-selector [executor-data & _] (:type executor-data))

(defmulti mk-threads executor-selector)
(defmulti mk-executor-stats executor-selector)
(defmulti close-component executor-selector)

(defn- normalized-component-conf [storm-conf general-context component-id]
  (let [to-remove (disj (set ALL-CONFIGS)
                        TOPOLOGY-DEBUG
                        TOPOLOGY-MAX-SPOUT-PENDING
                        TOPOLOGY-MAX-TASK-PARALLELISM
                        TOPOLOGY-TRANSACTIONAL-ID
                        TOPOLOGY-TICK-TUPLE-FREQ-SECS
                        TOPOLOGY-SLEEP-SPOUT-WAIT-STRATEGY-TIME-MS
                        TOPOLOGY-SPOUT-WAIT-STRATEGY
                        )
        spec-conf (-> general-context
                      (.getComponentCommon component-id)
                      .get_json_conf
                      from-json)]
    (merge storm-conf (apply dissoc spec-conf to-remove))
    ))

(defprotocol RunningExecutor
  (render-stats [this])
  (get-executor-id [this]))

(defn throttled-report-error-fn [executor]
  (let [storm-conf (:storm-conf executor)
        error-interval-secs (storm-conf TOPOLOGY-ERROR-THROTTLE-INTERVAL-SECS)
        max-per-interval (storm-conf TOPOLOGY-MAX-ERROR-REPORT-PER-INTERVAL)
        interval-start-time (atom (current-time-secs))
        interval-errors (atom 0)
        ]
    (fn [error]
      (log-error error)
      (when (> (time-delta @interval-start-time)
               error-interval-secs)
        (reset! interval-errors 0)
        (reset! interval-start-time (current-time-secs)))
      (swap! interval-errors inc)

      (when (<= @interval-errors max-per-interval)
        (cluster/report-error (:storm-cluster-state executor) (:storm-id executor) (:component-id executor) error)
        ))))

;; in its own function so that it can be mocked out by tracked topologies
(defn mk-executor-transfer-fn [batch-transfer->worker]
  (fn [task tuple]
    (disruptor/publish batch-transfer->worker [task tuple])))

(defn executor-data [worker executor-id]
  (let [worker-context (worker-context worker)
        task-ids (executor-id->tasks executor-id)
        component-id (.getComponentId worker-context (first task-ids))
        storm-conf (normalized-component-conf (:storm-conf worker) worker-context component-id)
        executor-type (executor-type worker-context component-id)
        batch-transfer->worker (disruptor/disruptor-queue
                                  (storm-conf TOPOLOGY-EXECUTOR-SEND-BUFFER-SIZE)
                                  :claim-strategy :single-threaded
                                  :wait-strategy (storm-conf TOPOLOGY-DISRUPTOR-WAIT-STRATEGY))
        ]
    (recursive-map
     :worker worker
     :worker-context worker-context
     :executor-id executor-id
     :task-ids task-ids
     :component-id component-id
     :storm-conf storm-conf
     :receive-queue ((:executor-receive-queue-map worker) executor-id)
     :storm-id (:storm-id worker)
     :conf (:conf worker)
     :shared-executor-data (HashMap.)
     :storm-active-atom (:storm-active-atom worker)
     :batch-transfer-queue batch-transfer->worker
     :transfer-fn (mk-executor-transfer-fn batch-transfer->worker)
     :suicide-fn (:suicide-fn worker)
     :storm-cluster-state (cluster/mk-storm-cluster-state (:cluster-state worker))
     :type executor-type
     ;; TODO: should refactor this to be part of the executor specific map (spout or bolt with :common field)
     :stats (mk-executor-stats <> (sampling-rate storm-conf))
     :task->component (:task->component worker)
     :stream->component->grouper (outbound-components worker-context component-id)
     :report-error (throttled-report-error-fn <>)
     :report-error-and-die (fn [error]
                             ((:report-error <>) error)
                             ((:suicide-fn <>)))
     :deserializer (KryoTupleDeserializer. storm-conf worker-context)
     :sampler (mk-stats-sampler storm-conf)
     ;; TODO: add in the executor-specific stuff in a :specific... or make a spout-data, bolt-data function?
     )))

(defn start-batch-transfer->worker-handler! [worker executor-data]
  (let [worker-transfer-fn (:transfer-fn worker)
        cached-emit (MutableObject. (ArrayList.))
        storm-conf (:storm-conf executor-data)
        serializer (KryoTupleSerializer. storm-conf (:worker-context executor-data))
        ]
    (disruptor/consume-loop*
      (:batch-transfer-queue executor-data)
      (disruptor/handler [o seq-id batch-end?]
        (let [^ArrayList alist (.getObject cached-emit)]
          (.add alist o)
          (when batch-end?
            (worker-transfer-fn serializer alist)
            (.setObject cached-emit (ArrayList.))
            )))
       :kill-fn (:report-error-and-die executor-data))))

(defn setup-ticks! [worker executor-data]
  (let [storm-conf (:storm-conf executor-data)
        tick-time-secs (storm-conf TOPOLOGY-TICK-TUPLE-FREQ-SECS)
        receive-queue (:receive-queue executor-data)
        context (:worker-context executor-data)]
    (when tick-time-secs
      (if (and (not (storm-conf TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS))
               (= :spout (:type executor-data)))
        (log-message "Timeouts disabled for executor " (:executor-id executor-data))
        (schedule-recurring
          (:user-timer worker)
          tick-time-secs
          tick-time-secs
          (fn []
            (disruptor/publish
              receive-queue
              [[nil (TupleImpl. context [tick-time-secs] -1 Constants/SYSTEM_TICK_STREAM_ID)]]
              )))))))

(defn mk-executor [worker executor-id]
  (let [executor-data (executor-data worker executor-id)
        _ (log-message "Loading executor " (:component-id executor-data) ":" (pr-str executor-id))
        task-datas (->> executor-data
                        :task-ids
                        (map (fn [t] [t (task/mk-task executor-data t)]))
                        (into {})
                        (HashMap.))
        _ (log-message "Loaded executor tasks " (:component-id executor-data) ":" (pr-str executor-id))
        report-error-and-die (:report-error-and-die executor-data)
        component-id (:component-id executor-data)

        ;; starting the batch-transfer->worker ensures that anything publishing to that queue 
        ;; doesn't block (because it's a single threaded queue and the caching/consumer started
        ;; trick isn't thread-safe)
        system-threads [(start-batch-transfer->worker-handler! worker executor-data)]
        handlers (with-error-reaction report-error-and-die
                   (mk-threads executor-data task-datas))
        threads (concat handlers system-threads)]    
    (setup-ticks! worker executor-data)
    
    (log-message "Finished loading executor " component-id ":" (pr-str executor-id))
    ;; TODO: add method here to get rendered stats... have worker call that when heartbeating
    (reify
      RunningExecutor
      (render-stats [this]
        (stats/render-stats! (:stats executor-data)))
      (get-executor-id [this]
        executor-id )
      Shutdownable
      (shutdown
        [this]
        (log-message "Shutting down executor " component-id ":" (pr-str executor-id))
        (disruptor/halt-with-interrupt! (:receive-queue executor-data))
        (disruptor/halt-with-interrupt! (:batch-transfer-queue executor-data))
        (doseq [t threads]
          (.interrupt t)
          (.join t))
        
        (doseq [user-context (map :user-context (vals task-datas))]
          (doseq [hook (.getHooks user-context)]
            (.cleanup hook)))
        (.disconnect (:storm-cluster-state executor-data))
        (doseq [obj (map :object (vals task-datas))]
          (close-component executor-data obj))
        (log-message "Shut down executor " component-id ":" (pr-str executor-id)))
        )))

(defn- fail-spout-msg [executor-data task-data msg-id tuple-info time-delta]
  (let [^ISpout spout (:object task-data)
        task-id (:task-id task-data)]
    ;;TODO: need to throttle these when there's lots of failures
    (log-debug "Failing message " msg-id ": " tuple-info)
    (.fail spout msg-id)
    (task/apply-hooks (:user-context task-data) .spoutFail (SpoutFailInfo. msg-id task-id time-delta))
    (when time-delta
      (stats/spout-failed-tuple! (:stats executor-data) (:stream tuple-info) time-delta)
      )))

(defn- ack-spout-msg [executor-data task-data msg-id tuple-info time-delta]
  (let [storm-conf (:storm-conf executor-data)
        ^ISpout spout (:object task-data)
        task-id (:task-id task-data)]
    (when (= true (storm-conf TOPOLOGY-DEBUG))
      (log-message "Acking message " msg-id))
    (.ack spout msg-id)
    (task/apply-hooks (:user-context task-data) .spoutAck (SpoutAckInfo. msg-id task-id time-delta))
    (when time-delta
      (stats/spout-acked-tuple! (:stats executor-data) (:stream tuple-info) time-delta)
      )))

(defn mk-task-receiver [executor-data tuple-action-fn]
  (let [^KryoTupleDeserializer deserializer (:deserializer executor-data)
        task-ids (:task-ids executor-data)
        debug? (= true (-> executor-data :storm-conf (get TOPOLOGY-DEBUG)))
        ]
    (disruptor/clojure-handler
      (fn [tuple-batch sequence-id end-of-batch?]
        (fast-list-iter [[task-id msg] tuple-batch]
          (let [^TupleImpl tuple (if (instance? Tuple msg) msg (.deserialize deserializer msg))]
            (when debug? (log-message "Processing received message " tuple))
            (if task-id
              (tuple-action-fn task-id tuple)
              ;; null task ids are broadcast tuples
              (fast-list-iter [task-id task-ids]
                (tuple-action-fn task-id tuple)
                ))
            ))))))

(defn executor-max-spout-pending [storm-conf num-tasks]
  (let [p (storm-conf TOPOLOGY-MAX-SPOUT-PENDING)]
    (if p (* p num-tasks))))

(defn init-spout-wait-strategy [storm-conf]
  (let [ret (-> storm-conf (get TOPOLOGY-SPOUT-WAIT-STRATEGY) new-instance)]
    (.prepare ret storm-conf)
    ret
    ))

(defmethod mk-threads :spout [executor-data task-datas]
  (let [wait-fn (fn [] @(:storm-active-atom executor-data))
        storm-conf (:storm-conf executor-data)
        ^ISpoutWaitStrategy spout-wait-strategy (init-spout-wait-strategy storm-conf)
        last-active (atom false)
        component-id (:component-id executor-data)
        max-spout-pending (executor-max-spout-pending storm-conf (count task-datas))
        ^Integer max-spout-pending (if max-spout-pending (int max-spout-pending))
        worker-context (:worker-context executor-data)
        transfer-fn (:transfer-fn executor-data)
        report-error-fn (:report-error executor-data)
        spouts (ArrayList. (map :object (vals task-datas)))
        sampler (:sampler executor-data)
        rand (Random. (Utils/secureRandomLong))
        
        pending (RotatingMap.
                 2 ;; microoptimize for performance of .size method
                 (reify RotatingMap$ExpiredCallback
                   (expire [this msg-id [task-id spout-id tuple-info start-time-ms]]
                     (let [time-delta (if start-time-ms (time-delta-ms start-time-ms))]
                       (fail-spout-msg executor-data (get task-datas task-id) spout-id tuple-info time-delta)
                       ))))
        tuple-action-fn (fn [task-id ^TupleImpl tuple]
                          (let [stream-id (.getSourceStreamId tuple)]
                            (if (= stream-id Constants/SYSTEM_TICK_STREAM_ID)
                              (.rotate pending)
                              (let [id (.getValue tuple 0)
                                    [stored-task-id spout-id tuple-finished-info start-time-ms] (.remove pending id)]
                                (when spout-id
                                  (when-not (= stored-task-id task-id)
                                    (throw-runtime "Fatal error, mismatched task ids: " task-id " " stored-task-id))
                                  (let [time-delta (if start-time-ms (time-delta-ms start-time-ms))]
                                    (condp = stream-id
                                        ACKER-ACK-STREAM-ID (ack-spout-msg executor-data (get task-datas task-id)
                                                              spout-id tuple-finished-info time-delta)
                                        ACKER-FAIL-STREAM-ID (fail-spout-msg executor-data (get task-datas task-id)
                                                               spout-id tuple-finished-info time-delta)                                    
                                        )))
                                ;; TODO: on failure, emit tuple to failure stream
                                ))))
        receive-queue (:receive-queue executor-data)
        event-handler (mk-task-receiver executor-data tuple-action-fn)
        has-ackers? (has-ackers? storm-conf)
        emitted-count (MutableLong. 0)
        empty-emit-streak (MutableLong. 0)]
    (log-message "Opening spout " component-id ":" (keys task-datas))
    (doseq [[task-id task-data] task-datas
            :let [^ISpout spout-obj (:object task-data)
                  tasks-fn (:tasks-fn task-data)
                  send-spout-msg (fn [out-stream-id values message-id out-task-id]
                                   (.increment emitted-count)
                                   (let [out-tasks (if out-task-id
                                                     (tasks-fn out-task-id out-stream-id values)
                                                     (tasks-fn out-stream-id values))
                                         rooted? (and message-id has-ackers?)
                                         root-id (if rooted? (MessageId/generateId rand))
                                         out-ids (fast-list-for [t out-tasks] (if rooted? (MessageId/generateId rand)))]
                                     (fast-list-iter [out-task out-tasks id out-ids]
                                       (let [tuple-id (if rooted?
                                                          (MessageId/makeRootId root-id id)
                                                          (MessageId/makeUnanchored))]                                                        
                                         (transfer-fn out-task
                                                      (TupleImpl. worker-context
                                                                  values
                                                                  task-id
                                                                  out-stream-id
                                                                  tuple-id))))
                                     (if rooted?
                                       (do
                                         (.put pending root-id [task-id
                                                                message-id
                                                                {:stream out-stream-id :values values}
                                                                (if (sampler) (System/currentTimeMillis))])
                                         (task/send-unanchored task-data
                                                               ACKER-INIT-STREAM-ID
                                                               [root-id (bit-xor-vals out-ids) task-id]))
                                       (when message-id
                                            (ack-spout-msg executor-data task-data message-id
                                                {:stream out-stream-id :values values}
                                                (if (sampler) 0))))
                                     (or out-tasks [])
                                     ))]]
      (.open spout-obj
             storm-conf
             (:user-context task-data)
             (SpoutOutputCollector.
               (reify ISpoutOutputCollector
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
               )))
    (log-message "Opened spout " component-id ":" (keys task-datas))
    [(async-loop
       (fn []
         (disruptor/consumer-started! (:receive-queue executor-data))
         (fn []
           ;; This design requires that spouts be non-blocking
           (disruptor/consume-batch receive-queue event-handler)
           (let [active? (wait-fn)
                 curr-count (.get emitted-count)]
             (if (or (not max-spout-pending)
                     (< (.size pending) max-spout-pending))
               (if active?
                 (do
                   (when-not @last-active
                     (reset! last-active true)
                     (log-message "Activating spout " component-id ":" (keys task-datas))
                     (fast-list-iter [^ISpout spout spouts] (.activate spout)))
               
                   (fast-list-iter [^ISpout spout spouts] (.nextTuple spout)))
                 (do
                   (when @last-active
                     (reset! last-active false)
                     (log-message "Deactivating spout " component-id ":" (keys task-datas))
                     (fast-list-iter [^ISpout spout spouts] (.deactivate spout)))
                   ;; TODO: log that it's getting throttled
                   (Time/sleep 100))))
             (if (and (= curr-count (.get emitted-count)) active?)
                (do (.increment empty-emit-streak)
                    (.emptyEmit spout-wait-strategy (.get empty-emit-streak)))
                (.set empty-emit-streak 0)
                ))
            0 ))
      :kill-fn (:report-error-and-die executor-data)
      :factory? true
      )]
    ))

(defn- tuple-time-delta! [^TupleImpl tuple]
  (let [ms (.getSampleStartTime tuple)]
    (if ms
      (time-delta-ms ms))))

(defn put-xor! [^Map pending key id]
  (let [curr (or (.get pending key) (long 0))]
    (.put pending key (bit-xor curr id))))

(defmethod mk-threads :bolt [executor-data task-datas]
  (let [component-id (:component-id executor-data)
        transfer-fn (:transfer-fn executor-data)
        worker-context (:worker-context executor-data)
        storm-conf (:storm-conf executor-data)
        executor-stats (:stats executor-data)
        report-error-fn (:report-error executor-data)
        sampler (:sampler executor-data)
        rand (Random. (Utils/secureRandomLong))
        tuple-action-fn (fn [task-id ^TupleImpl tuple]
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
 
                          ;;(log-debug "Received tuple " tuple " at task " task-id)
                          ;; need to do it this way to avoid reflection
                          (let [^IBolt bolt-obj (->> task-id (get task-datas) :object)]
                            (when (sampler)
                              (.setSampleStartTime tuple (System/currentTimeMillis)))
                            (.execute bolt-obj tuple)))]
    (log-message "Preparing bolt " component-id ":" (keys task-datas))
    (doseq [[task-id task-data] task-datas
            :let [^IBolt bolt-obj (:object task-data)
                  tasks-fn (:tasks-fn task-data)
                  user-context (:user-context task-data)
                  bolt-emit (fn [stream anchors values task]
                              (let [out-tasks (if task
                                                (tasks-fn task stream values)
                                                (tasks-fn stream values))]
                                (fast-list-iter [t out-tasks]
                                  (let [anchors-to-ids (HashMap.)]
                                    (fast-list-iter [^TupleImpl a anchors]
                                      (let [root-ids (-> a .getMessageId .getAnchorsToIds .keySet)]
                                        (when (pos? (count root-ids))
                                          (let [edge-id (MessageId/generateId rand)]
                                            (.updateAckVal a edge-id)
                                            (fast-list-iter [root-id root-ids]
                                              (put-xor! anchors-to-ids root-id edge-id))
                                              ))))
                                    (transfer-fn t
                                                 (TupleImpl. worker-context
                                                             values
                                                             task-id
                                                             stream
                                                             (MessageId/makeId anchors-to-ids)))))
                                (or out-tasks [])))]]
      (.prepare bolt-obj
                storm-conf
                user-context
                (OutputCollector.
                  (reify IOutputCollector
                     (emit [this stream anchors values]
                       (bolt-emit stream anchors values nil))
                     (emitDirect [this task stream anchors values]
                       (bolt-emit stream anchors values task))
                     (^void ack [this ^Tuple tuple]
                       (let [^TupleImpl tuple tuple
                             ack-val (.getAckVal tuple)]
                         (fast-map-iter [[root id] (.. tuple getMessageId getAnchorsToIds)]
                           (task/send-unanchored task-data
                                                 ACKER-ACK-STREAM-ID
                                                 [root (bit-xor id ack-val)])
                           ))
                       (let [delta (tuple-time-delta! tuple)]
                         (task/apply-hooks user-context .boltAck (BoltAckInfo. tuple task-id delta))
                         (when delta
                           (stats/bolt-acked-tuple! executor-stats
                                                    (.getSourceComponent tuple)
                                                    (.getSourceStreamId tuple)
                                                    delta)
                           )))
                     (^void fail [this ^Tuple tuple]
                       (fast-list-iter [root (.. tuple getMessageId getAnchors)]
                         (task/send-unanchored task-data
                                               ACKER-FAIL-STREAM-ID
                                               [root]))
                       (let [delta (tuple-time-delta! tuple)]
                         (task/apply-hooks user-context .boltFail (BoltFailInfo. tuple task-id delta))
                         (when delta
                           (stats/bolt-failed-tuple! executor-stats
                                                     (.getSourceComponent tuple)
                                                     (.getSourceStreamId tuple)
                                                     delta)
                           )))
                     (reportError [this error]
                       (report-error-fn error)
                       )))))

    (log-message "Prepared bolt " component-id ":" (keys task-datas))
    ;; TODO: can get any SubscribedState objects out of the context now
    [(disruptor/consume-loop*
      (:receive-queue executor-data)
      (mk-task-receiver executor-data tuple-action-fn)
      :kill-fn (:report-error-and-die executor-data))]
    ))


(defmethod close-component :spout [executor-data spout]
  (.close spout))

(defmethod close-component :bolt [executor-data bolt]
  (.cleanup bolt))

;; TODO: refactor this to be part of an executor-specific map
(defmethod mk-executor-stats :spout [_ rate]
  (stats/mk-spout-stats rate))

(defmethod mk-executor-stats :bolt [_ rate]
  (stats/mk-bolt-stats rate))
