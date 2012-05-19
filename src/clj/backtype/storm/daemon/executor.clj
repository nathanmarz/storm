(ns backtype.storm.daemon.executor
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap])
  (:import [java.util.concurrent ConcurrentLinkedQueue ConcurrentHashMap LinkedBlockingQueue])
  (:import [backtype.storm.hooks ITaskHook])
  (:import [backtype.storm.tuple Tuple])
  (:import [backtype.storm.hooks.info SpoutAckInfo SpoutFailInfo
              EmitInfo BoltFailInfo BoltAckInfo])
  (:require [backtype.storm [tuple :as tuple]])
  (:require [backtype.storm.daemon [task :as task]])
  )

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

(defn- mk-custom-grouper [^CustomStreamGrouping grouping ^WorkerTopologyContext context ^Fields out-fields target-tasks]
  (.prepare grouping context out-fields target-tasks)
  (fn [^List values]
    (.chooseTasks grouping values)
    ))

(defn- mk-grouper
  "Returns a function that returns a vector of which task indices to send tuple to, or just a single task index."
  [^WorkerTopologyContext context ^Fields out-fields thrift-grouping ^List target-tasks]
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

(defn outbound-components
  "Returns map of stream id to component id to grouper"
  [^WorkerTopologyContext worker-context component-id]
  (let [output-groupings (clojurify-structure (.getTargets worker-context component-id))]
     (into {}
       (for [[stream-id component->grouping] output-groupings
             :let [out-fields (.getComponentOutputFields worker-context component-id stream-id)
                   component->grouping (filter-key #(-> worker-context
                                                        (.getComponentTasks %)
                                                        count
                                                        pos?)
                                                    component->grouping)]]         
         [stream-id
          (into {}
                (for [[component tgrouping] component->grouping]
                  [component (mk-grouper worker-context
                                         out-fields
                                         tgrouping
                                         (.getComponentTasks worker-context component)
                                         )]
                  ))]))))



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
                        TOPOLOGY-TRANSACTIONAL-ID)
        spec-conf (-> general-context
                      (.getComponentCommon component-id)
                      .get_json_conf
                      from-json)]
    (merge storm-conf (apply dissoc spec-conf to-remove))
    ))

(defprotocol RunningExecutor
  (render-stats [this])
  (get-executor-id [this]))

(defn report-error [executor error]
  (log-error error)
  (cluster/report-error (:storm-cluster-state executor) (:storm-id executor) (:component-id executor) error))

(defn executor-data [worker executor-id]
  (let [worker-context (worker-context worker)
        task-ids (executor-id->tasks executor-id)
        component-id (.getComponentId worker-context (first task-ids))
        storm-conf (normalized-component-conf (:storm-conf worker) worker-context component-id)
        executor-type (executor-type worker-context component-id)]
    (recursive-map
     :worker worker
     :worker-context worker-context
     :task-ids task-ids
     :component-id component-id
     :storm-conf storm-conf
     :receive-queue ((:receive-queue-map worker) (first task-ids))
     :storm-id (:storm-id worker)
     :conf (:conf worker)
     :storm-active-atom (:storm-active-atom worker)
     :transfer-fn (:transfer-fn worker)
     :suicide-fn (:suicide-fn worker)
     :storm-cluster-state (cluster/mk-storm-cluster-state (:cluster-state worker))
     :type executor-type
     ;; TODO: should refactor this to be part of the executor specific map (spout or bolt with :common field)
     :stats (mk-executor-stats <> (sampling-rate storm-conf))
     :task->component (:task->component worker)
     :stream->component->grouper (outbound-components worker-context component-id)
     :report-error (partial report-error <>)
     :report-error-and-die (fn [error]
                             ((:report-error <>) error)
                             ((:suicide-fn <>)))
     :deserializer (KryoTupleDeserializer. storm-conf worker-context)
     :sampler (mk-stats-sampler storm-conf)
     ;; TODO: add in the executor-specific stuff in a :specific... or make a spout-data, bolt-data function?
     )))

(defn mk-executor [worker executor-id]
  (let [executor-data (executor-data worker executor-id)
        _ (log-message "Loading executor " (:component-id executor-data) ":" (pr-str executor-id))
        active (atom true)
        task-datas (->> executor-data
                        :task-ids
                        (map (fn [t] [t (task/mk-task executor-data t)]))
                        (into {}))
        report-error-and-die (:report-error-and-die executor-data)
        component-id (:component-id executor-data)

        
        threads (dofor [exec (with-error-reaction report-error-and-die
                                (mk-threads executor-data task-datas))]
                  (async-loop (fn [] (exec) (when @active 0))
                              :kill-fn report-error-and-die))]
    (log-message "Finished loading executor " component-id ":" (keys task-datas))
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
        (log-message "Shutting down executor " component-id ":" (keys task-datas))
        (reset! active false)
        ;; put an empty message into receive-queue
        ;; empty messages are skip messages (this unblocks the receive-queue.take thread)
        (.put (:receive-queue executor-data) (byte-array []))
        (doseq [t threads]
          (.interrupt t)
          (.join t))
        
        (doseq [user-context (map :user-context (vals task-datas))]
          (doseq [hook (.getHooks user-context)]
            (.cleanup hook)))
        (.disconnect (:storm-cluster-state executor-data))
        (doseq [obj (map :object (vals task-datas))]
          (close-component executor-data obj))
        (log-message "Shut down executor " component-id ":" (keys task-datas)))
        )))

(defn- fail-spout-msg [executor-data task-data msg-id tuple-info time-delta]
  (let [^ISpout spout (:object task-data)]
    ;;TODO: need to throttle these when there's lots of failures
    (log-message "Failing message " msg-id ": " tuple-info)
    (.fail spout msg-id)
    (task/apply-hooks (:user-context task-data) .spoutFail (SpoutFailInfo. msg-id time-delta))
    (when ((:sampler executor-data))
      (stats/spout-failed-tuple! (:stats executor-data) (:stream tuple-info) time-delta)
      )))

(defn- ack-spout-msg [executor-data task-data msg-id tuple-info time-delta]
  (let [storm-conf (:storm-conf executor-data)
        ^ISpout spout (:object task-data)]
    (when (= true (storm-conf TOPOLOGY-DEBUG))
      (log-message "Acking message " msg-id))
    (.ack spout msg-id)
    (task/apply-hooks (:user-context task-data) .spoutAck (SpoutAckInfo. msg-id time-delta))
    (when ((:sampler executor-data))
      (stats/spout-acked-tuple! (:stats executor-data) (:stream tuple-info) time-delta)
      )))

(defn mk-task-receiver [executor-data tuple-action-fn]
  (let [^LinkedBlockingQueue receive-queue (:receive-queue executor-data)
        ^KryoTupleDeserializer deserializer (:deserializer executor-data)]
    (fn []
      (let [[task-id msg] (.take receive-queue)
            is-tuple? (instance? Tuple msg)]
        (when (or is-tuple? (not (empty? msg))) ; skip empty messages (used during shutdown)
          (log-debug "Processing message " msg)
          (let [^Tuple tuple (if is-tuple? msg (.deserialize deserializer msg))]
            (tuple-action-fn task-id tuple)
            ))
        ))))

(defmethod mk-threads :spout [executor-data task-datas]
  (let [wait-fn (fn [] @(:storm-active-atom executor-data))
        storm-conf (:storm-conf executor-data)
        last-active (atom false)
        component-id (:component-id executor-data)
        max-spout-pending (* (storm-conf TOPOLOGY-MAX-SPOUT-PENDING) (count task-datas))
        event-queue (ConcurrentLinkedQueue.)
        worker-context (:worker-context executor-data)
        transfer-fn (:transfer-fn executor-data)
        report-error-fn (:report-error-fn executor-data)
        spouts (map :object (vals task-datas))
        
        pending (TimeCacheMap.
                 (int (storm-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS))
                 (reify TimeCacheMap$ExpiredCallback
                   (expire [this msg-id [task-id spout-id tuple-info start-time-ms]]
                     (let [time-delta (time-delta-ms start-time-ms)]
                       (.add event-queue #(fail-spout-msg executor-data (task-datas task-id) spout-id tuple-info time-delta)))
                     )))
        send-spout-msg (fn [from-task-id out-stream-id values message-id out-task-id]
                         (let [task-data (task-datas from-task-id)
                               tasks-fn (:tasks-fn task-data)
                               out-tasks (if out-task-id
                                           (tasks-fn out-task-id out-stream-id values)
                                           (tasks-fn out-stream-id values))
                               root-id (MessageId/generateId)
                               rooted? (and message-id (has-ackers? storm-conf))
                               out-ids (dofor [t out-tasks] (MessageId/generateId))
                               out-tuples (dofor [id out-ids]
                                            (let [tuple-id (if rooted?
                                                             (MessageId/makeRootId root-id id)
                                                             (MessageId/makeUnanchored))]
                                              (Tuple. worker-context
                                                      values
                                                      from-task-id
                                                      out-stream-id
                                                      tuple-id)))]
                           (dorun
                            (map transfer-fn out-tasks out-tuples))
                           (if rooted?
                             (do
                               (.put pending root-id [from-task-id
                                                      message-id
                                                      {:stream out-stream-id :values values}
                                                      (System/currentTimeMillis)])
                               (task/send-unanchored (task-datas from-task-id)
                                                     ACKER-INIT-STREAM-ID
                                                     [root-id (bit-xor-vals out-ids) from-task-id]))
                             (when message-id
                               (.add event-queue #(ack-spout-msg executor-data task-data message-id {:stream out-stream-id :values values} 0))))
                           (or out-tasks [])
                           ))
        tuple-action-fn (fn [task-id ^Tuple tuple]
                          (let [id (.getValue tuple 0)
                                [stored-task-id spout-id tuple-finished-info start-time-ms] (.remove pending id)]
                            (when-not (= stored-task-id task-id)
                              (throw-runtime "Fatal error, mismatched task ids: " task-id " " stored-task-id))
                            (when spout-id
                              (let [time-delta (time-delta-ms start-time-ms)]
                                (condp = (.getSourceStreamId tuple)
                                    ACKER-ACK-STREAM-ID (.add event-queue #(ack-spout-msg executor-data (task-datas task-id)
                                                                              spout-id tuple-finished-info time-delta))
                                    ACKER-FAIL-STREAM-ID (.add event-queue #(fail-spout-msg executor-data (task-datas task-id)
                                                                                  spout-id tuple-finished-info time-delta))
                                    )))
                            ;; TODO: on failure, emit tuple to failure stream
                            ))]
    (log-message "Opening spout " component-id ":" (keys task-datas))
    (doseq [[task-id task-data] task-datas]      
      (.open ^ISpout (:object task-data)
             storm-conf
             (:user-context task-data)
             (SpoutOutputCollector.
               (reify ISpoutOutputCollector
                 (^List emit [this ^String stream-id ^List tuple ^Object message-id]
                   (send-spout-msg task-id stream-id tuple message-id nil)
                   )
                 (^void emitDirect [this ^int out-task-id ^String stream-id
                                    ^List tuple ^Object message-id]
                   (send-spout-msg task-id stream-id tuple message-id out-task-id)
                   )
                 (reportError [this error]
                   (report-error-fn error)
                   ))
               )))
    (log-message "Opened spout " component-id ":" (keys task-datas))
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
               (log-message "Activating spout " component-id ":" (keys task-datas))
               (doseq [^ISpout spout spouts] (.activate spout)))
              (doseq [^ISpout spout spouts] (.nextTuple spout)))
           (do
             (when @last-active
               (reset! last-active false)
               (log-message "Deactivating spout " component-id ":" (keys task-datas))
               (doseq [^ISpout spout spouts] (.activate spout)))
             ;; TODO: log that it's getting throttled
             (Time/sleep 100)))
         ))
         (mk-task-receiver executor-data tuple-action-fn)
     ]
    ))

(defn- tuple-time-delta! [^Map start-times ^Tuple tuple]
  (time-delta-ms (.remove start-times tuple)))

(defn put-xor! [^Map pending key id]
  (let [curr (or (.get pending key) (long 0))]
    ;; TODO: this portion is not thread safe (multiple threads updating same value at same time)
    (.put pending key (bit-xor curr id))))

(defmethod mk-threads :bolt [executor-data task-datas]
  (let [component-id (:component-id executor-data)
        tuple-start-times (ConcurrentHashMap.)
        transfer-fn (:transfer-fn executor-data)
        worker-context (:worker-context executor-data)
        storm-conf (:storm-conf executor-data)
        executor-stats (:stats executor-data)
        pending-acks (ConcurrentHashMap.)
        report-error-fn (:report-error-fn executor-data)
        sampler (:sampler executor-data)
        bolt-emit (fn [from-task-id stream anchors values task]
                    (let [task-data (task-datas from-task-id)
                          tasks-fn (:tasks-fn task-data)
                          out-tasks (if task
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
                                     (Tuple. worker-context
                                             values
                                             from-task-id
                                             stream
                                             (MessageId/makeId anchors-to-ids))))
                      (or out-tasks [])))
        tuple-action-fn (fn [task-id ^Tuple tuple]
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
 
                          (log-debug "Received tuple " tuple " at task " task-id)
                          (.put tuple-start-times tuple (System/currentTimeMillis))                          
                          (.execute ^IBolt (-> task-id task-datas :object) tuple))]
    (log-message "Preparing bolt " component-id ":" (keys task-datas))
    (doseq [[task-id task-data] task-datas]
      (.prepare ^IBolt (:object task-data)
                storm-conf
                (:user-context task-data)
                (OutputCollector.
                  (reify IOutputCollector
                     (emit [this stream anchors values]
                       (bolt-emit task-id stream anchors values nil))
                     (emitDirect [this task stream anchors values]
                       (bolt-emit task-id stream anchors values task))
                     (^void ack [this ^Tuple tuple]
                       (let [ack-val (or (.remove pending-acks tuple) (long 0))]
                         (doseq [[root id] (.. tuple getMessageId getAnchorsToIds)]
                           (task/send-unanchored task-data
                                                 ACKER-ACK-STREAM-ID
                                                 [root (bit-xor id ack-val)])
                           ))
                       (let [delta (tuple-time-delta! tuple-start-times tuple)]
                         (task/apply-hooks (:user-context task-data) .boltAck (BoltAckInfo. tuple delta))
                         (when (sampler)
                           (stats/bolt-acked-tuple! executor-stats
                                                    (.getSourceComponent tuple)
                                                    (.getSourceStreamId tuple)
                                                    delta)
                           )))
                     (^void fail [this ^Tuple tuple]
                       (.remove pending-acks tuple)
                       (doseq [root (.. tuple getMessageId getAnchors)]
                         (task/send-unanchored task-data
                                               ACKER-FAIL-STREAM-ID
                                               [root]))
                       (let [delta (tuple-time-delta! tuple-start-times tuple)]
                         (task/apply-hooks (:user-context task-data) .boltFail (BoltFailInfo. tuple delta))
                         (when (sampler)
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
    [(mk-task-receiver executor-data tuple-action-fn)]
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
