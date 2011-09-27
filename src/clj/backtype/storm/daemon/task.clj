(ns backtype.storm.daemon.task
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap])
  (:import [java.util.concurrent ConcurrentLinkedQueue ConcurrentHashMap])
  (:require [backtype.storm [tuple :as tuple]]))

(bootstrap)

(defn- mk-fields-grouper [^Fields out-fields ^Fields group-fields num-tasks]
  (fn [^Tuple tuple]
    (mod (tuple/list-hash-code (.select out-fields group-fields (.getValues tuple)))
         num-tasks)
    ))

(defn- mk-grouper
  "Returns a function that returns a vector of which task indices to send tuple to, or just a single task index."
  [^Fields out-fields thrift-grouping num-tasks]
  (let [random (Random.)]
    (condp = (thrift/grouping-type thrift-grouping)
      :fields
        (if (thrift/global-grouping? thrift-grouping)
          (fn [^Tuple tuple]
            ;; It's possible for target to have multiple tasks if it reads multiple sources
            0 )
          (let [group-fields (Fields. (thrift/field-grouping thrift-grouping))]
            (mk-fields-grouper out-fields group-fields num-tasks)
            ))
      :all
        (fn [^Tuple tuple]
          (range num-tasks))
      :shuffle
        (let [choices (rotating-random-range num-tasks)]
          (fn [^Tuple tuple]
            (acquire-random-range-id choices num-tasks)
            ))
      :none
        (fn [^Tuple tuple]
          (mod (.nextInt random) num-tasks))
      :direct
        :direct
      )))

(defn- update-ack [curr-entry val]
  (let [old (get curr-entry :val 0)]
    (assoc curr-entry :val (bit-xor old val))
    ))

(defn- acker-emit-direct [^OutputCollector collector ^Integer task ^Integer stream ^List values]
  (.emitDirect collector task stream values)
  )

(defn mk-acker-bolt []
  (let [output-collector (atom nil)
        pending (atom nil)]
    (reify IBolt
      (^void prepare [this ^Map storm-conf ^TopologyContext context ^OutputCollector collector]
               (reset! output-collector collector)
               (reset! pending (TimeCacheMap. (int (storm-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS))))
               )
      (^void execute [this ^Tuple tuple]
             (let [id (.getValue tuple 0)
                   ^TimeCacheMap pending @pending
                   curr (.get pending id)
                   curr (condp = (.getSourceStreamId tuple)
                            ACKER-INIT-STREAM-ID (-> curr
                                                     (update-ack id)
                                                     (assoc :spout-task (.getValue tuple 1)))
                            ACKER-ACK-STREAM-ID (update-ack curr (.getValue tuple 1))
                            ACKER-FAIL-STREAM-ID (assoc curr :failed true))]
               (.put pending id curr)
               (when (and curr
                          (:spout-task curr))
                 (cond (= 0 (:val curr))
                       (do
                         (.remove pending id)
                         (acker-emit-direct @output-collector
                                            (:spout-task curr)
                                            ACKER-ACK-STREAM-ID
                                            [id]
                                            ))
                       (:failed curr)
                       (do
                         (.remove pending id)
                         (acker-emit-direct @output-collector
                                            (:spout-task curr)
                                            ACKER-FAIL-STREAM-ID
                                            [id]
                                            ))
                       ))
               (.ack ^OutputCollector @output-collector tuple)
               ))
      (^void cleanup [this]
             )
      )))

(defn- get-task-object [topology component-id]
  (if (= ACKER-COMPONENT-ID component-id)
    (mk-acker-bolt)
    (let [spouts (.get_spouts topology)
          bolts (.get_bolts topology)
          obj (Utils/getSetComponentObject
               (cond
                (contains? spouts component-id) (.get_spout_object (get spouts component-id))
                (contains? bolts component-id) (.get_bolt_object (get bolts component-id))
                true (throw (RuntimeException. (str "Could not find " component-id " in " topology)))))
          obj (if (instance? ShellComponent obj)
                (if (contains? spouts component-id)
                  (ShellSpout. obj)
                  (ShellBolt. obj))
                obj )]
      obj
      )))


(defn outbound-components
  "Returns map of stream id to component id to grouper"
  [topology-context]
  (let [output-groupings (clojurify-structure (.getThisTargets topology-context))
        acker-task-amt (count (.getComponentTasks topology-context ACKER-COMPONENT-ID))]
    (merge
     {
      ACKER-INIT-STREAM-ID
        {ACKER-COMPONENT-ID (mk-fields-grouper (Fields. ["id" "spout-task"])
                                               (Fields. ["id"])
                                               acker-task-amt)}
      ACKER-ACK-STREAM-ID
        {ACKER-COMPONENT-ID (mk-fields-grouper (Fields. ["id" "ack-val"])
                                               (Fields. ["id"])
                                               acker-task-amt)}
      ACKER-FAIL-STREAM-ID
        {ACKER-COMPONENT-ID (mk-fields-grouper (Fields. ["id"]) ;; TODO: add failure msg here later...
                                               (Fields. ["id"])
                                               acker-task-amt)}
      }
     (into {}
           (for [[stream-id component->grouping] output-groupings
                 :let [out-fields (.getThisOutputFields topology-context stream-id)]]
             [stream-id
              (into {}
                    (for [[component tgrouping] component->grouping]
                      [component (mk-grouper out-fields
                                             tgrouping
                                             (count (.getComponentTasks topology-context component))
                                             )]
                      ))])))
    ))



(defmulti mk-executors class-selector)
(defmulti close-component class-selector)
(defmulti mk-task-stats class-selector)

(defn- get-readable-name [topology-context]
  (let [component-id (.getThisComponentId topology-context)]
    (if (system-component? component-id)
      ({ACKER-COMPONENT-ID "Acker"} component-id)
      ;; TODO: change this so that can get better name for nested bolts 
      (str (class (get-task-object (.getRawTopology topology-context) component-id)))
      )))

(defn- send-ack [^TopologyContext topology-context ^Tuple input-tuple
                 ^List generated-ids send-fn]
  (let [ack-val (bit-xor-vals generated-ids)]
    (doseq [[anchor id] (.. input-tuple getMessageId getAnchorsToIds)]
      (send-fn (Tuple. topology-context
                       [anchor (bit-xor ack-val id)]
                       (.getThisTaskId topology-context)
                       ACKER-ACK-STREAM-ID))
      )))

(defn mk-task [conf storm-conf topology-context storm-id mq-context cluster-state storm-active-atom transfer-fn]
  (let [task-id (.getThisTaskId topology-context)
        component-id (.getThisComponentId topology-context)
        task-info (.getTaskToComponent topology-context)
        active (atom true)
        uptime (uptime-computer)
        storm-cluster-state (cluster/mk-storm-cluster-state cluster-state)
        
        task-object (get-task-object (.getRawTopology topology-context)
                                     (.getThisComponentId topology-context))
        task-stats (mk-task-stats task-object (sampling-rate storm-conf))

        report-error (fn [error]
                       (.report-task-error storm-cluster-state storm-id task-id error)
                       (halt-process! 1 "Task died!"))

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
                          :kill-fn report-error)

        stream->component->grouper (outbound-components topology-context)
        component->tasks (reverse-map task-info)
        ;; important it binds to virtual port before function returns
        puller (msg/bind mq-context task-id)

        ;; TODO: consider DRYing things up and moving stats / tuple -> multiple components code here
        task-transfer-fn (fn [task ^Tuple tuple]
                           (transfer-fn task tuple)
                           )
        task-readable-name (get-readable-name topology-context)

        emit-sampler (mk-stats-sampler storm-conf)
        send-fn (fn this
                  ([^Integer out-task-id ^Tuple tuple]
                     (when (= true (storm-conf TOPOLOGY-DEBUG))
                       (log-message "Emitting direct: " out-task-id "; " task-readable-name " " tuple))
                     (let [target-component (.getComponentId topology-context out-task-id)
                           component->grouping (stream->component->grouper (.getSourceStreamId tuple))
                           grouping (get component->grouping target-component)
                           out-task-id (if (or
                                            ;; This makes streams to/from system
                                            ;; component (like ackers) implicit
                                            (system-component? component-id)
                                            (system-component? target-component)
                                            grouping)
                                         out-task-id)]
                       (when (and (not-nil? grouping) (not= :direct grouping))
                         (throw (IllegalArgumentException. "Cannot emitDirect to a task expecting a regular grouping")))
                       (when out-task-id
                         (task-transfer-fn out-task-id tuple))
                       (when (emit-sampler)
                         (stats/emitted-tuple! task-stats (.getSourceStreamId tuple))
                         (stats/transferred-tuples! task-stats (.getSourceStreamId tuple) 1)
                         )
                       [out-task-id]
                       ))
                  ([^Tuple tuple]
                     (when (= true (storm-conf TOPOLOGY-DEBUG))
                       (log-message "Emitting: " task-readable-name " " tuple))
                     (let [stream (.getSourceStreamId tuple)
                           ;; TODO: this doesn't seem to be very fast
                           ;; and seems to be the current bottleneck
                           out-tasks (mapcat
                                      (fn [[out-component grouper]]
                                        (when (= :direct grouper)
                                          ;;  TODO: this is wrong, need to check how the stream was declared
                                          (throw (IllegalArgumentException. "Cannot do regular emit to direct stream")))
                                        (let [tasks (component->tasks out-component)
                                              indices (collectify (grouper tuple))]
                                          (for [i indices] (tasks i))))
                                      (stream->component->grouper stream))
                           num-out-tasks (count out-tasks)]
                       (when (emit-sampler)
                         (stats/emitted-tuple! task-stats (.getSourceStreamId tuple))
                         (stats/transferred-tuples! task-stats (.getSourceStreamId tuple) num-out-tasks)
                         )
                       (if (= num-out-tasks 1)
                         (task-transfer-fn (first out-tasks) tuple)
                         ;;TODO: optimize the out-tasks = 0 case by
                         ;; not including this tuple in the ack list
                         ;; for previous tuple
                         ;; TODO: need to create the new ids, and then create the tuples, and then ack
                         (let [out-ids (repeatedly (count out-tasks) #(MessageId/generateId))]
                           (dorun
                            (map (fn [id t]
                                   (task-transfer-fn t (.copyWithNewId tuple id)))
                                 out-ids
                                 out-tasks))
                           (send-ack topology-context
                                     tuple
                                     out-ids
                                     this)
                           ))
                       out-tasks)))
        executor-threads (dofor
                          [exec (mk-executors task-object storm-conf puller send-fn
                                              storm-active-atom topology-context
                                              task-stats report-error)]
                          (async-loop (fn [] (exec) (when @active 0))
                                      :kill-fn report-error))
        system-threads [heartbeat-thread]
        all-threads  (concat executor-threads system-threads)]
    (reify
       Shutdownable
       (shutdown
        [this]
        (log-message "Shutting down task " storm-id ":" task-id)
        (reset! active false)
        ;; empty messages are skip messages (this unblocks the socket)
        (msg/send-local-task-empty mq-context task-id)
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

(defn- fail-spout-msg [^ISpout spout storm-conf msg-id ^Tuple tuple time-delta task-stats]
  (log-message "Failing message " msg-id ": " tuple)
  (.fail spout msg-id)
  (when time-delta
    (stats/spout-failed-tuple! task-stats (.getSourceStreamId tuple) time-delta)
    ))

(defn- ack-spout-msg [^ISpout spout storm-conf msg-id ^Tuple tuple time-delta task-stats]
  (when (= true (storm-conf TOPOLOGY-DEBUG))
    (log-message "Acking message " msg-id))
  (.ack spout msg-id)
  (when time-delta
    (stats/spout-acked-tuple! task-stats (.getSourceStreamId tuple) time-delta)
    ))

(defmethod mk-executors ISpout [^ISpout spout storm-conf puller send-fn storm-active-atom
                                ^TopologyContext topology-context task-stats report-error-fn]
  (let [wait-fn (fn [] @storm-active-atom)
        max-spout-pending (storm-conf TOPOLOGY-MAX-SPOUT-PENDING)
        deserializer (TupleDeserializer. storm-conf topology-context)
        event-queue (ConcurrentLinkedQueue.)
        sampler (mk-stats-sampler storm-conf)
        pending (TimeCacheMap.
                 (int (storm-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS))
                 (reify TimeCacheMap$ExpiredCallback
                        (expire [this msg-id [spout-id tuple start-time-ms]]
                                (let [time-delta (if start-time-ms (time-delta-ms start-time-ms))]
                                  (.add event-queue #(fail-spout-msg spout storm-conf spout-id tuple time-delta task-stats)))
                                )))
        send-spout-msg (fn [out-stream-id values message-id out-task-id]
                         (let [task-id (.getThisTaskId topology-context)
                               gen-id (MessageId/generateId)
                               tuple-id (if message-id
                                          (MessageId/makeRootId gen-id)
                                          (MessageId/makeUnanchored))
                               tuple (Tuple. topology-context
                                             values
                                             task-id
                                             out-stream-id
                                             tuple-id)
                               out-tasks (if out-task-id
                                           (send-fn out-task-id tuple)
                                           (send-fn tuple))]
                           (if (= 0 (storm-conf TOPOLOGY-ACKERS))
                             (.add event-queue #(ack-spout-msg spout storm-conf message-id tuple nil task-stats))
                             (when message-id
                               (.put pending gen-id [message-id
                                                    tuple
                                                    (if (sampler) (System/currentTimeMillis))])
                               (send-fn (Tuple. topology-context
                                                [gen-id task-id]
                                                task-id
                                                ACKER-INIT-STREAM-ID))
                               ))
                           out-tasks
                           ))
        output-collector (reify ISpoutOutputCollector
                                (^List emit [this ^int stream-id ^List tuple ^Object message-id]
                                       (send-spout-msg stream-id tuple message-id nil)
                                       )
                                (^void emitDirect [this ^int out-task-id ^int stream-id
                                                   ^List tuple ^Object message-id]
                                       (send-spout-msg stream-id tuple message-id out-task-id)
                                       ))]
    (.open spout storm-conf topology-context (SpoutOutputCollector. output-collector))
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
                  [spout-id tuple-finished start-time-ms] (.remove pending id)
                  time-delta (if start-time-ms (time-delta-ms start-time-ms))]
              (when spout-id
                (condp = (.getSourceStreamId tuple)
                    ACKER-ACK-STREAM-ID (.add event-queue #(ack-spout-msg spout storm-conf spout-id
                                                                          tuple-finished time-delta task-stats))
                    ACKER-FAIL-STREAM-ID (.add event-queue #(fail-spout-msg spout storm-conf spout-id
                                                                            tuple-finished time-delta task-stats))
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

(defmethod mk-executors IBolt [^IBolt bolt storm-conf puller send-fn storm-active-atom
                               ^TopologyContext topology-context task-stats report-error-fn]
  (let [deserializer (TupleDeserializer. storm-conf topology-context)
        task-id (.getThisTaskId topology-context)
        component-id (.getThisComponentId topology-context)
        tuple-start-times (ConcurrentHashMap.)
        sampler (mk-stats-sampler storm-conf)
        output-collector (reify IInternalOutputCollector
                          (^List emit [this ^Tuple output]
                                 (send-fn output)
                                 )
                          (^void emitDirect [this ^int task-id ^Tuple output]
                                 (send-fn task-id output)
                                 )
                          
                          (^void ack [this ^Tuple input-tuple ^List generated-ids]
                                 (send-ack topology-context
                                           input-tuple
                                           generated-ids
                                           send-fn)
                                 (let [delta (tuple-time-delta! tuple-start-times input-tuple)]
                                   (when delta
                                     (stats/bolt-acked-tuple! task-stats
                                                              (.getSourceComponent input-tuple)
                                                              (.getSourceStreamId input-tuple)
                                                              delta)
                                     )))
                          (^void fail [this ^Tuple input-tuple]
                                 (doseq [anchor (.. input-tuple getMessageId getAnchors)]
                                     (send-fn (Tuple. topology-context
                                                      [anchor]
                                                      task-id
                                                      ACKER-FAIL-STREAM-ID))
                                     )
                                 (let [delta (tuple-time-delta! tuple-start-times input-tuple)]
                                   (when delta
                                     (stats/bolt-failed-tuple! task-stats
                                                               (.getSourceComponent input-tuple)
                                                               (.getSourceStreamId input-tuple)
                                                               delta)
                                     )))
                          (^void reportError [this ^Throwable error]
                                 (report-error-fn error)
                                 ))]
    (.prepare bolt
              storm-conf
              topology-context
              (OutputCollectorImpl. topology-context output-collector))
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
         (when-not (empty? ser)  ; skip empty messages (used during shutdown)
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
