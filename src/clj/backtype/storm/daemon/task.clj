(ns backtype.storm.daemon.task
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap])
  (:import [java.util.concurrent ConcurrentLinkedQueue ConcurrentHashMap])
  (:require [backtype.storm [tuple :as tuple]]))

(bootstrap)

(defn- mk-fields-grouper [^Fields out-fields ^Fields group-fields num-tasks]
  (fn [^List values]
    (mod (tuple/list-hash-code (.select out-fields group-fields values))
         num-tasks)
    ))

(defn- mk-custom-grouper [^CustomStreamGrouping grouping ^Fields out-fields num-tasks]
  (.prepare grouping out-fields num-tasks)
  (fn [^List values]
    (.taskIndices grouping values)
    ))

(defn- mk-grouper
  "Returns a function that returns a vector of which task indices to send tuple to, or just a single task index."
  [^Fields out-fields thrift-grouping num-tasks]
  (let [random (Random.)]
    (condp = (thrift/grouping-type thrift-grouping)
      :fields
        (if (thrift/global-grouping? thrift-grouping)
          (fn [tuple]
            ;; It's possible for target to have multiple tasks if it reads multiple sources
            0 )
          (let [group-fields (Fields. (thrift/field-grouping thrift-grouping))]
            (mk-fields-grouper out-fields group-fields num-tasks)
            ))
      :all
        (fn [tuple]
          (range num-tasks))
      :shuffle
        (let [choices (rotating-random-range num-tasks)]
          (fn [tuple]
            (acquire-random-range-id choices num-tasks)
            ))
      :none
        (fn [tuple]
          (mod (.nextInt random) num-tasks))
      :custom-object
        (let [grouping (thrift/instantiate-java-object (.get_custom_object thrift-grouping))]
          (mk-custom-grouper grouping out-fields num-tasks))
      :custom-serialized
        (let [grouping (Utils/deserialize (.get_custom_serialized thrift-grouping))]
          (mk-custom-grouper grouping out-fields num-tasks))
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


(defn outbound-components
  "Returns map of stream id to component id to grouper"
  [^TopologyContext topology-context]
  (let [output-groupings (clojurify-structure (.getThisTargets topology-context))]
     (into {}
       (for [[stream-id component->grouping] output-groupings
             :let [out-fields (.getThisOutputFields topology-context stream-id)
                   component->grouping (filter-key #(pos? (count (.getComponentTasks topology-context %)))
                                       component->grouping)]]         
         [stream-id
          (into {}
                (for [[component tgrouping] component->grouping]
                  [component (mk-grouper out-fields
                                         tgrouping
                                         (count (.getComponentTasks topology-context component))
                                         )]
                  ))]))
    ))



(defmulti mk-executors class-selector)
(defmulti close-component class-selector)
(defmulti mk-task-stats class-selector)

(defn- get-readable-name [topology-context]
  (.getThisComponentId topology-context))

(defn- component-conf [storm-conf topology-context component-id]
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

(defn mk-task [conf storm-conf topology-context user-context storm-id mq-context cluster-state storm-active-atom transfer-fn suicide-fn]
  (let [task-id (.getThisTaskId topology-context)
        component-id (.getThisComponentId topology-context)
        storm-conf (component-conf storm-conf topology-context component-id)
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
                       (.report-task-error storm-cluster-state storm-id task-id error))
        
        report-error-and-die (fn [error]
                               (report-error error)
                               (suicide-fn))

        ;; heartbeat ASAP so nimbus doesn't reassign
        heartbeat-thread (async-loop
                          (fn []
                            (.task-heartbeat! storm-cluster-state storm-id task-id
                                              (TaskHeartbeat. (current-time-secs)
                                                              (uptime)
                                                              (stats/render-stats! task-stats)))
                            (when @active (storm-conf TASK-HEARTBEAT-FREQUENCY-SECS))
                            )
                          :priority Thread/MAX_PRIORITY
                          :kill-fn report-error-and-die)

        stream->component->grouper (outbound-components topology-context)
        component->tasks (reverse-map task-info)
        ;; important it binds to virtual port before function returns
        puller (msg/bind mq-context storm-id task-id)

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
                                         (let [tasks (component->tasks out-component)
                                               indices (collectify (grouper values))]
                                           (for [i indices] (tasks i))))
                                       (stream->component->grouper stream))]
                        (when (emit-sampler)
                          (stats/emitted-tuple! task-stats stream)
                          (stats/transferred-tuples! task-stats stream (count out-tasks)))
                        out-tasks)))
        _ (send-unanchored topology-context tasks-fn transfer-fn SYSTEM-STREAM-ID ["startup"])
        executor-threads (dofor
                          [exec (with-error-reaction report-error-and-die
                                  (mk-executors task-object storm-conf puller tasks-fn
                                                transfer-fn
                                                storm-active-atom topology-context
                                                user-context task-stats report-error))]
                          (async-loop (fn [] (exec) (when @active 0))
                                      :kill-fn report-error-and-die))
        system-threads [heartbeat-thread]
        all-threads  (concat executor-threads system-threads)]
    (log-message "Finished loading task " component-id ":" task-id)
    (reify
      Shutdownable
      (shutdown
        [this]
        (log-message "Shutting down task " storm-id ":" task-id)
        (reset! active false)
        ;; empty messages are skip messages (this unblocks the socket)
        (msg/send-local-task-empty mq-context storm-id task-id)
        (doseq [t all-threads]
          (.interrupt t)
          (.join t))
        (.remove-task-heartbeat! storm-cluster-state storm-id task-id)
        (.disconnect storm-cluster-state)
        (.close puller)
        (close-component task-object)
        (log-message "Shut down task " storm-id ":" task-id))
      DaemonCommon
      (waiting? [this]
        ;; executor threads are independent since they don't sleep
        ;; -> they block on zeromq
        (every? (memfn sleeping?) system-threads)
        ))))

(defn- fail-spout-msg [^ISpout spout storm-conf msg-id tuple-info time-delta task-stats]
  (log-message "Failing message " msg-id ": " tuple-info)
  (.fail spout msg-id)
  (when time-delta
    (stats/spout-failed-tuple! task-stats (:stream tuple-info) time-delta)
    ))

(defn- ack-spout-msg [^ISpout spout storm-conf msg-id tuple-info time-delta task-stats]
  (when (= true (storm-conf TOPOLOGY-DEBUG))
    (log-message "Acking message " msg-id))
  (.ack spout msg-id)
  (when time-delta
    (stats/spout-acked-tuple! task-stats (:stream tuple-info) time-delta)
    ))

(defmethod mk-executors ISpout [^ISpout spout storm-conf puller tasks-fn transfer-fn storm-active-atom
                                ^TopologyContext topology-context ^TopologyContext user-context
                                task-stats report-error-fn]
  (let [wait-fn (fn [] @storm-active-atom)
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
                     (let [time-delta (if start-time-ms (time-delta-ms start-time-ms))]
                       (.add event-queue #(fail-spout-msg spout storm-conf spout-id tuple-info time-delta task-stats)))
                     )))
        send-spout-msg (fn [out-stream-id values message-id out-task-id]
                         (let [out-tasks (if out-task-id
                                           (tasks-fn out-stream-id values out-task-id)
                                           (tasks-fn out-stream-id values))
                               root-id (MessageId/generateId)
                               rooted? (and message-id (> (storm-conf TOPOLOGY-ACKERS) 0))
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
                                                      (if (sampler) (System/currentTimeMillis))])
                               (send-unanchored topology-context tasks-fn transfer-fn
                                                ACKER-INIT-STREAM-ID
                                                [root-id (bit-xor-vals out-ids) task-id]))
                             (when message-id
                               (.add event-queue #(ack-spout-msg spout storm-conf message-id {:stream out-stream-id :values values} nil task-stats))))
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
                             ))]
    (log-message "Opening spout " component-id ":" task-id)
    (.open spout storm-conf user-context (SpoutOutputCollector. output-collector))
    (log-message "Opened spout " component-id ":" task-id)
    [(fn []
       ;; This design requires that spouts be non-blocking
       (loop []
         (when-let [event (.poll event-queue)]
           (event)
           (recur)
           ))
       (if (or (not max-spout-pending)
               (< (.size pending) max-spout-pending))
         (if (wait-fn)
           (.nextTuple spout)
           (Time/sleep 100))
         ;; TODO: log that it's getting throttled
         ))
     (fn []
       (let [^bytes ser-msg (msg/recv puller)]
         ;; skip empty messages (used during shutdown)
         (when-not (empty? ser-msg)
           (let [tuple (.deserialize deserializer ser-msg)
                 id (.getValue tuple 0)
                 [spout-id tuple-finished-info start-time-ms] (.remove pending id)
                 time-delta (if start-time-ms (time-delta-ms start-time-ms))]
             (when spout-id
               (condp = (.getSourceStreamId tuple)
                 ACKER-ACK-STREAM-ID (.add event-queue #(ack-spout-msg spout storm-conf spout-id
                                                                       tuple-finished-info time-delta task-stats))
                 ACKER-FAIL-STREAM-ID (.add event-queue #(fail-spout-msg spout storm-conf spout-id
                                                                         tuple-finished-info time-delta task-stats))
                 )))
           ;; TODO: on failure, emit tuple to failure stream
           )))
     ]
    ))

(defn- tuple-time-delta! [^Map start-times ^Tuple tuple]
  (let [start-time (.remove start-times tuple)]
    (if start-time
      (time-delta-ms start-time))
    ))

(defn put-xor! [^Map pending key id]
  (let [curr (or (.get pending key) (long 0))]
    ;; TODO: this portion is not thread safe (multiple threads updating same value at same time)
    (.put pending key (bit-xor curr id))))

(defmethod mk-executors IBolt [^IBolt bolt storm-conf puller tasks-fn transfer-fn storm-active-atom
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
                               (when delta
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
                               (when delta
                                 (stats/bolt-failed-tuple! task-stats
                                                           (.getSourceComponent tuple)
                                                           (.getSourceStreamId tuple)
                                                           delta)
                                 )))
                           (reportError [this error]
                             (report-error-fn error)
                             ))]
    (log-message "Preparing bolt " component-id ":" task-id)
    (.prepare bolt
              storm-conf
              user-context
              (OutputCollector. output-collector))
    (log-message "Prepared bolt " component-id ":" task-id)
    ;; TODO: can get any SubscribedState objects out of the context now
    [(fn []
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
       (let [^bytes ser (msg/recv puller)]
         (when-not (empty? ser) ; skip empty messages (used during shutdown)
           (log-debug "Processing message")
           (let [tuple (.deserialize deserializer ser)]
             ;; TODO: for state sync, need to check if tuple comes from state spout. if so, update state
             ;; TODO: how to handle incremental updates as well as synchronizations at same time
             ;; TODO: need to version tuples somehow
             (log-debug "Received tuple " tuple " at task " (.getThisTaskId topology-context))
             (when (sampler)
               (.put tuple-start-times tuple (System/currentTimeMillis)))
             
             (.execute bolt tuple)
             ))))]
    ))

(defmethod close-component ISpout [spout]
  (.close spout))

(defmethod close-component IBolt [bolt]
  (.cleanup bolt))

(defmethod mk-task-stats ISpout [_ rate]
  (stats/mk-spout-stats rate))

(defmethod mk-task-stats IBolt [_ rate]
  (stats/mk-bolt-stats rate))
