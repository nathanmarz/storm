;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns org.apache.storm.daemon.executor
  (:use [org.apache.storm.daemon common])
  (:use [clojure.walk])
  (:import [org.apache.storm.generated Grouping Grouping$_Fields]
           [java.io Serializable]
           [org.apache.storm.stats BoltExecutorStats SpoutExecutorStats]
           [org.apache.storm.daemon.metrics BuiltinMetricsUtil SpoutThrottlingMetrics])
  (:use [org.apache.storm util config log])
  (:import [java.util List Random HashMap ArrayList LinkedList Map])
  (:import [org.apache.storm ICredentialsListener Thrift])
  (:import [org.apache.storm.hooks ITaskHook])
  (:import [org.apache.storm.tuple AddressedTuple Tuple Fields TupleImpl MessageId])
  (:import [org.apache.storm.spout ISpoutWaitStrategy ISpout SpoutOutputCollector ISpoutOutputCollector])
  (:import [org.apache.storm.hooks.info SpoutAckInfo SpoutFailInfo
            EmitInfo BoltFailInfo BoltAckInfo BoltExecuteInfo])
  (:import [org.apache.storm.grouping CustomStreamGrouping])
  (:import [org.apache.storm.task WorkerTopologyContext IBolt OutputCollector IOutputCollector])
  (:import [org.apache.storm.generated GlobalStreamId])
  (:import [org.apache.storm.utils Utils ConfigUtils TupleUtils MutableObject RotatingMap RotatingMap$ExpiredCallback MutableLong Time DisruptorQueue WorkerBackpressureThread DisruptorBackpressureCallback])
  (:import [com.lmax.disruptor InsufficientCapacityException])
  (:import [org.apache.storm.serialization KryoTupleSerializer])
  (:import [org.apache.storm.daemon Shutdownable StormCommon Acker Task GrouperFactory])
  (:import [org.apache.storm.metric.api IMetric IMetricsConsumer$TaskInfo IMetricsConsumer$DataPoint StateMetric])
  (:import [org.apache.storm Config Constants])
  (:import [org.apache.storm.cluster ClusterStateContext DaemonType StormClusterStateImpl ClusterUtils])
  (:import [org.apache.storm.grouping LoadAwareCustomStreamGrouping LoadAwareShuffleGrouping LoadMapping ShuffleGrouping])
  (:import [java.lang Thread Thread$UncaughtExceptionHandler]
           [java.util.concurrent ConcurrentLinkedQueue]
           [org.json.simple JSONValue]
           [com.lmax.disruptor.dsl ProducerType]
           [org.apache.storm StormTimer])
  (:require [clojure.set :as set]))


;TODO: when translating this function, you should replace the filter-val with a proper for loop + if condition HERE
(defn- outbound-groupings
  [^WorkerTopologyContext worker-context this-component-id stream-id out-fields component->grouping topo-conf]
  (->> component->grouping
       (filter-key #(-> worker-context
                        (.getComponentTasks %)
                        count
                        pos?))
       (map (fn [[component tgrouping]]
               [component
                (GrouperFactory/mkGrouper worker-context
                                this-component-id
                                stream-id
                                out-fields
                                tgrouping
                                (.getComponentTasks worker-context component)
                                topo-conf)]))
       (into {})
       (HashMap.)))

(defn outbound-components
  "Returns map of stream id to component id to grouper"
  [^WorkerTopologyContext worker-context component-id topo-conf]
  (->> (.getTargets worker-context component-id)
        clojurify-structure
        (map (fn [[stream-id component->grouping]]
               [stream-id
                (outbound-groupings
                  worker-context
                  component-id
                  stream-id
                  (.getComponentOutputFields worker-context component-id stream-id)
                  component->grouping
                  topo-conf)]))
         (into (apply merge (map #(hash-map % nil) (.keySet (.get_streams (.getComponentCommon worker-context component-id))))))
         (HashMap.)))

(defn executor-type [^WorkerTopologyContext context component-id]
  (let [topology (.getRawTopology context)
        spouts (.get_spouts topology)
        bolts (.get_bolts topology)]
    (cond (contains? spouts component-id) "spout"
          (contains? bolts component-id) "bolt"
          :else (throw (RuntimeException. (str "Could not find " component-id " in topology " topology))))))

(defn executor-selector [executor-data & _] (keyword (:type executor-data)))

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
                        TOPOLOGY-BOLTS-WINDOW-LENGTH-COUNT
                        TOPOLOGY-BOLTS-WINDOW-LENGTH-DURATION-MS
                        TOPOLOGY-BOLTS-SLIDING-INTERVAL-COUNT
                        TOPOLOGY-BOLTS-SLIDING-INTERVAL-DURATION-MS
                        TOPOLOGY-BOLTS-TUPLE-TIMESTAMP-FIELD-NAME
                        TOPOLOGY-BOLTS-TUPLE-TIMESTAMP-MAX-LAG-MS
                        TOPOLOGY-BOLTS-MESSAGE-ID-FIELD-NAME
                        TOPOLOGY-STATE-PROVIDER
                        TOPOLOGY-STATE-PROVIDER-CONFIG
                        )
        spec-conf (-> general-context
                      (.getComponentCommon component-id)
                      .get_json_conf
                      (#(if % (JSONValue/parse %)))
                      clojurify-structure)]
    (merge storm-conf (apply dissoc spec-conf to-remove))
    ))

(defprotocol RunningExecutor
  (render-stats [this])
  (get-executor-id [this])
  (credentials-changed [this creds])
  (get-backpressure-flag [this]))

(defn throttled-report-error-fn [executor]
  (let [storm-conf (:storm-conf executor)
        error-interval-secs (storm-conf TOPOLOGY-ERROR-THROTTLE-INTERVAL-SECS)
        max-per-interval (storm-conf TOPOLOGY-MAX-ERROR-REPORT-PER-INTERVAL)
        interval-start-time (atom (Time/currentTimeSecs))
        interval-errors (atom 0)
        ]
    (fn [error]
      (log-error error)
      (when (> (Time/deltaSecs @interval-start-time)
               error-interval-secs)
        (reset! interval-errors 0)
        (reset! interval-start-time (Time/currentTimeSecs)))
      (swap! interval-errors inc)

      (when (<= @interval-errors max-per-interval)
        (.reportError (:storm-cluster-state executor) (:storm-id executor) (:component-id executor)
                              (Utils/hostname storm-conf)
          (long (.getThisWorkerPort (:worker-context executor))) error)
        ))))

;; in its own function so that it can be mocked out by tracked topologies
(defn mk-executor-transfer-fn [batch-transfer->worker storm-conf]
  (fn this
    [task tuple]
    (let [val (AddressedTuple. task tuple)]
      (when (= true (storm-conf TOPOLOGY-DEBUG))
        (log-message "TRANSFERING tuple " val))
      (.publish ^DisruptorQueue batch-transfer->worker val))))

(defn mk-executor-data [worker executor-id]
  (let [worker-context (worker-context worker)
        task-ids (clojurify-structure (StormCommon/executorIdToTasks executor-id))
        component-id (.getComponentId worker-context (first task-ids))
        storm-conf (normalized-component-conf (:storm-conf worker) worker-context component-id)
        executor-type (executor-type worker-context component-id)
        batch-transfer->worker (DisruptorQueue.
                                  (str "executor"  executor-id "-send-queue")
                                  ProducerType/SINGLE
                                  (storm-conf TOPOLOGY-EXECUTOR-SEND-BUFFER-SIZE)
                                  (storm-conf TOPOLOGY-DISRUPTOR-WAIT-TIMEOUT-MILLIS)
                                  (storm-conf TOPOLOGY-DISRUPTOR-BATCH-SIZE)
                                  (storm-conf TOPOLOGY-DISRUPTOR-BATCH-TIMEOUT-MILLIS))
        ]
    (recursive-map
     :worker worker
     :worker-context worker-context
     :executor-id executor-id
     :task-ids task-ids
     :component-id component-id
     :open-or-prepare-was-called? (atom false)
     :storm-conf storm-conf
     :receive-queue ((:executor-receive-queue-map worker) executor-id)
     :storm-id (:storm-id worker)
     :conf (:conf worker)
     :shared-executor-data (HashMap.)
     :storm-active-atom (:storm-active-atom worker)
     :storm-component->debug-atom (:storm-component->debug-atom worker)
     :batch-transfer-queue batch-transfer->worker
     :transfer-fn (mk-executor-transfer-fn batch-transfer->worker storm-conf)
     :suicide-fn (:suicide-fn worker)
     :storm-cluster-state (ClusterUtils/mkStormClusterState (:state-store worker) (Utils/getWorkerACL storm-conf)
                            (ClusterStateContext. DaemonType/WORKER))
     :type executor-type
     ;; TODO: should refactor this to be part of the executor specific map (spout or bolt with :common field)
     :stats (mk-executor-stats <> (ConfigUtils/samplingRate storm-conf))
     :interval->task->metric-registry (HashMap.)
     :task->component (:task->component worker)
     :stream->component->grouper (outbound-components worker-context component-id storm-conf)
     :report-error (throttled-report-error-fn <>)
     :report-error-and-die (reify
                             Thread$UncaughtExceptionHandler
                             (uncaughtException [this _ error]
                               ((:report-error <>) error)
                               (if (or
                                    (Utils/exceptionCauseIsInstanceOf InterruptedException error)
                                    (Utils/exceptionCauseIsInstanceOf java.io.InterruptedIOException error))
                                 (log-message "Got interrupted excpetion shutting thread down...")
                                 ((:suicide-fn <>)))))
     :sampler (ConfigUtils/mkStatsSampler storm-conf)
     :backpressure (atom false)
     :spout-throttling-metrics (if (= (keyword executor-type) :spout)
                                 (SpoutThrottlingMetrics.)
                                nil)
     ;; TODO: add in the executor-specific stuff in a :specific... or make a spout-data, bolt-data function?
     )))

(defn- mk-disruptor-backpressure-handler [executor-data]
  "make a handler for the executor's receive disruptor queue to
  check highWaterMark and lowWaterMark for backpressure"
  (reify DisruptorBackpressureCallback
    (highWaterMark [this]
      "When receive queue is above highWaterMark"
      (if (not @(:backpressure executor-data))
        (do (reset! (:backpressure executor-data) true)
            (log-debug "executor " (:executor-id executor-data) " is congested, set backpressure flag true")
            (WorkerBackpressureThread/notifyBackpressureChecker (:backpressure-trigger (:worker executor-data))))))
    (lowWaterMark [this]
      "When receive queue is below lowWaterMark"
      (if @(:backpressure executor-data)
        (do (reset! (:backpressure executor-data) false)
            (log-debug "executor " (:executor-id executor-data) " is not-congested, set backpressure flag false")
            (WorkerBackpressureThread/notifyBackpressureChecker (:backpressure-trigger (:worker executor-data))))))))

(defn start-batch-transfer->worker-handler! [worker executor-data]
  (let [worker-transfer-fn (:transfer-fn worker)
        cached-emit (MutableObject. (ArrayList.))
        storm-conf (:storm-conf executor-data)
        serializer (KryoTupleSerializer. storm-conf (:worker-context executor-data))
        ^DisruptorQueue batch-transfer-queue (:batch-transfer-queue executor-data)
        handler (reify com.lmax.disruptor.EventHandler
                  (onEvent [this o seq-id batch-end?]
                    (let [^ArrayList alist (.getObject cached-emit)]
                      (.add alist o)
                      (when batch-end?
                        (worker-transfer-fn serializer alist)
                        (.setObject cached-emit (ArrayList.))))))
        ]
    (Utils/asyncLoop
      (fn [] (.consumeBatchWhenAvailable batch-transfer-queue handler) 0)
      (.getName batch-transfer-queue)
      (:uncaught-exception-handler (:report-error-and-die executor-data)))))

;; TODO: this is all expensive... should be precomputed
(defn send-unanchored
  [^Task task-data stream values transfer-fn]
  (let [out-tuple (.getTuple task-data stream values)]
    (fast-list-iter [t (.getOutgoingTasks task-data stream values)]
                    (transfer-fn t out-tuple))))

(defn setup-metrics! [executor-data]
  (let [{:keys [storm-conf receive-queue worker-context interval->task->metric-registry]} executor-data
        distinct-time-bucket-intervals (keys interval->task->metric-registry)]
    (doseq [interval distinct-time-bucket-intervals]
      (.scheduleRecurring
        (:user-timer (:worker executor-data))
        interval
        interval
        (fn []
          (let [val [(AddressedTuple. AddressedTuple/BROADCAST_DEST (TupleImpl. worker-context [interval] Constants/SYSTEM_TASK_ID Constants/METRICS_TICK_STREAM_ID))]]
            (.publish ^DisruptorQueue receive-queue val)))))))

(defn metrics-tick
  [executor-data task-data ^TupleImpl tuple]
   (let [{:keys [interval->task->metric-registry ^WorkerTopologyContext worker-context]} executor-data
         interval (.getInteger tuple 0)
         transfer-fn (:transfer-fn executor-data)
         task-id (.getTaskId task-data)
         name->imetric (-> interval->task->metric-registry (get interval) (get task-id))
         task-info (IMetricsConsumer$TaskInfo.
                     (Utils/hostname (:storm-conf executor-data))
                     (.getThisWorkerPort worker-context)
                     (:component-id executor-data)
                     task-id
                     (long (/ (System/currentTimeMillis) 1000))
                     interval)
         data-points (->> name->imetric
                          (map (fn [[name imetric]]
                                 (let [value (.getValueAndReset ^IMetric imetric)]
                                   (if value
                                     (IMetricsConsumer$DataPoint. name value)))))
                          (filter identity)
                          (into []))]
     (when (seq data-points)
       (send-unanchored task-data Constants/METRICS_STREAM_ID [task-info data-points] transfer-fn))))

(defn setup-ticks! [worker executor-data]
  (let [storm-conf (:storm-conf executor-data)
        tick-time-secs (storm-conf TOPOLOGY-TICK-TUPLE-FREQ-SECS)
        receive-queue (:receive-queue executor-data)
        context (:worker-context executor-data)]
    (when tick-time-secs
      (if (or (Utils/isSystemId (:component-id executor-data))
              (and (= false (storm-conf TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS))
                   (= :spout (keyword (:type executor-data)))))
        (log-message "Timeouts disabled for executor " (:component-id executor-data) ":" (:executor-id executor-data))
        (.scheduleRecurring
          (:user-timer worker)
          tick-time-secs
          tick-time-secs
          (fn []
            (let [val [(AddressedTuple. AddressedTuple/BROADCAST_DEST (TupleImpl. context [tick-time-secs] Constants/SYSTEM_TASK_ID Constants/SYSTEM_TICK_STREAM_ID))]]
              (.publish ^DisruptorQueue receive-queue val))))))))

(defn mk-executor [worker executor-id initial-credentials]
  (let [executor-data (mk-executor-data worker executor-id)
        transfer-fn (:transfer-fn executor-data)
        _ (log-message "Loading executor " (:component-id executor-data) ":" (pr-str executor-id))
        task-datas (->> executor-data
                        :task-ids
                        (map (fn [t] (let [task (Task. (stringify-keys executor-data)  t)
                                           stream StormCommon/SYSTEM_STREAM_ID
                                           values ["startup"]]
                                       ;; when this is called, the threads for the executor haven't been started yet,
                                       ;; so we won't be risking trampling on the single-threaded claim strategy disruptor queue
                                       (send-unanchored task stream values transfer-fn)
                                       [t task]
                                       )))
                        (into {})
                        (HashMap.))
        _ (log-message "Loaded executor tasks " (:component-id executor-data) ":" (pr-str executor-id))
        report-error-and-die (:report-error-and-die executor-data)
        component-id (:component-id executor-data)


        disruptor-handler (mk-disruptor-backpressure-handler executor-data)
        _ (.registerBackpressureCallback (:receive-queue executor-data) disruptor-handler)
        _ (-> (.setHighWaterMark (:receive-queue executor-data) ((:storm-conf executor-data) BACKPRESSURE-DISRUPTOR-HIGH-WATERMARK))
              (.setLowWaterMark ((:storm-conf executor-data) BACKPRESSURE-DISRUPTOR-LOW-WATERMARK))
              (.setEnableBackpressure ((:storm-conf executor-data) TOPOLOGY-BACKPRESSURE-ENABLE)))

        ;; starting the batch-transfer->worker ensures that anything publishing to that queue 
        ;; doesn't block (because it's a single threaded queue and the caching/consumer started
        ;; trick isn't thread-safe)
        system-threads [(start-batch-transfer->worker-handler! worker executor-data)]
        handlers (try
                   (mk-threads executor-data task-datas initial-credentials)
                   (catch Throwable t (.uncaughtException report-error-and-die nil t)))
        threads (concat handlers system-threads)]    
    (setup-ticks! worker executor-data)

    (log-message "Finished loading executor " component-id ":" (pr-str executor-id))
    ;; TODO: add method here to get rendered stats... have worker call that when heartbeating
    (reify
      RunningExecutor
      (render-stats [this]
        (.renderStats (:stats executor-data)))
      (get-executor-id [this]
        executor-id)
      (credentials-changed [this creds]
        (let [receive-queue (:receive-queue executor-data)
              context (:worker-context executor-data)
              val [(AddressedTuple. AddressedTuple/BROADCAST_DEST (TupleImpl. context [creds] Constants/SYSTEM_TASK_ID Constants/CREDENTIALS_CHANGED_STREAM_ID))]]
          (.publish ^DisruptorQueue receive-queue val)))
      (get-backpressure-flag [this]
        @(:backpressure executor-data))
      Shutdownable
      (shutdown
        [this]
        (log-message "Shutting down executor " component-id ":" (pr-str executor-id))
        (.haltWithInterrupt ^DisruptorQueue (:receive-queue executor-data))
        (.haltWithInterrupt ^DisruptorQueue (:batch-transfer-queue executor-data))
        (doseq [t threads]
          (.interrupt t)
          (.join t))

        (.cleanupStats (:stats executor-data))
        (doseq [user-context (map #(.getUserContext %) (vals task-datas))]
          (doseq [hook (.getHooks user-context)]
            (.cleanup hook)))
        (.disconnect (:storm-cluster-state executor-data))
        (when @(:open-or-prepare-was-called? executor-data)
          (doseq [obj (map #(.getTaskObject %) (vals task-datas))]
            (close-component executor-data obj)))
        (log-message "Shut down executor " component-id ":" (pr-str executor-id)))
        )))

(defn- fail-spout-msg [executor-data task-data msg-id tuple-info time-delta reason id]
  (let [^ISpout spout (.getTaskObject task-data)
        storm-conf (:storm-conf executor-data)
        task-id (.getTaskId task-data)]
    ;;TODO: need to throttle these when there's lots of failures
    (when (= true (storm-conf TOPOLOGY-DEBUG))
      (log-message "SPOUT Failing " id ": " tuple-info " REASON: " reason " MSG-ID: " msg-id))
    (.fail spout msg-id)
    (.applyOn (SpoutFailInfo. msg-id task-id time-delta) (.getUserContext task-data))
    (when time-delta
      (.spoutFailedTuple (:stats executor-data) (:stream tuple-info) time-delta))))

(defn- ack-spout-msg [executor-data task-data msg-id tuple-info time-delta id]
  (let [storm-conf (:storm-conf executor-data)
        ^ISpout spout (.getTaskObject task-data)
        task-id (.getTaskId task-data)]
    (when (= true (storm-conf TOPOLOGY-DEBUG))
      (log-message "SPOUT Acking message " id " " msg-id))
    (.ack spout msg-id)
    (.applyOn (SpoutAckInfo. msg-id task-id time-delta) (.getUserContext task-data))
    (when time-delta
      (.spoutAckedTuple (:stats executor-data) (:stream tuple-info) time-delta))))

(defn mk-task-receiver [executor-data tuple-action-fn]
  (let [task-ids (:task-ids executor-data)
        debug? (= true (-> executor-data :storm-conf (get TOPOLOGY-DEBUG)))
        ]
    (reify com.lmax.disruptor.EventHandler
      (onEvent [this tuple-batch sequence-id end-of-batch?]
        (fast-list-iter [^AddressedTuple addressed-tuple tuple-batch]
          (let [^TupleImpl tuple (.getTuple addressed-tuple)
                task-id (.getDest addressed-tuple)]
            (when debug? (log-message "Processing received message FOR " task-id " TUPLE: " tuple))
            (if (not= task-id AddressedTuple/BROADCAST_DEST)
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
  (let [ret (-> storm-conf (get TOPOLOGY-SPOUT-WAIT-STRATEGY) Utils/newInstance)]
    (.prepare ret storm-conf)
    ret
    ))

;; Send sampled data to the eventlogger if the global or component level
;; debug flag is set (via nimbus api).
(defn send-to-eventlogger [executor-data task-data values component-id message-id random]
    (let [c->d @(:storm-component->debug-atom executor-data)
          options (get c->d component-id (get c->d (:storm-id executor-data)))
          spct    (if (and (not-nil? options) (:enable options)) (:samplingpct options) 0)]
      ;; the thread's initialized random number generator is used to generate
      ;; uniformily distributed random numbers.
      (when (and (> spct 0) (< (* 100 (.nextDouble random)) spct))
        (send-unanchored
          task-data
          StormCommon/EVENTLOGGER_STREAM_ID
          [component-id message-id (System/currentTimeMillis) values]
          (:transfer-fn executor-data)))))

(defmethod mk-threads :spout [executor-data task-datas initial-credentials]
  (let [{:keys [storm-conf component-id worker-context transfer-fn report-error sampler open-or-prepare-was-called?]} executor-data
        ^ISpoutWaitStrategy spout-wait-strategy (init-spout-wait-strategy storm-conf)
        max-spout-pending (executor-max-spout-pending storm-conf (count task-datas))
        ^Integer max-spout-pending (if max-spout-pending (int max-spout-pending))        
        last-active (atom false)        
        spouts (ArrayList. (map #(.getTaskObject %) (vals task-datas)))
        rand (Random. (Utils/secureRandomLong))
        ^DisruptorQueue transfer-queue (executor-data :batch-transfer-queue)
        debug? (= true (storm-conf TOPOLOGY-DEBUG))

        pending (RotatingMap.
                 2 ;; microoptimize for performance of .size method
                 (reify RotatingMap$ExpiredCallback
                   (expire [this id [task-id spout-id tuple-info start-time-ms]]
                     (let [time-delta (if start-time-ms (Time/deltaMs start-time-ms))]
                       (fail-spout-msg executor-data (get task-datas task-id) spout-id tuple-info time-delta "TIMEOUT" id)
                       ))))
        tuple-action-fn (fn [task-id ^TupleImpl tuple]
                          (let [stream-id (.getSourceStreamId tuple)]
                            (condp = stream-id
                              Constants/SYSTEM_TICK_STREAM_ID (.rotate pending)
                              Constants/METRICS_TICK_STREAM_ID (metrics-tick executor-data (get task-datas task-id) tuple)
                              Constants/CREDENTIALS_CHANGED_STREAM_ID 
                                (let [task-data (get task-datas task-id)
                                      spout-obj (.getTaskObject task-data)]
                                  (when (instance? ICredentialsListener spout-obj)
                                    (.setCredentials spout-obj (.getValue tuple 0))))
                              Acker/ACKER_RESET_TIMEOUT_STREAM_ID 
                                (let [id (.getValue tuple 0)
                                      pending-for-id (.get pending id)]
                                   (when pending-for-id
                                     (.put pending id pending-for-id))) 
                              (let [id (.getValue tuple 0)
                                    [stored-task-id spout-id tuple-finished-info start-time-ms] (.remove pending id)]
                                (when spout-id
                                  (when-not (= stored-task-id task-id)
                                    (throw (RuntimeException. (str "Fatal error, mismatched task ids: " task-id " " stored-task-id))))
                                  (let [time-delta (if start-time-ms (Time/deltaMs start-time-ms))]
                                    (condp = stream-id
                                      Acker/ACKER_ACK_STREAM_ID (ack-spout-msg executor-data (get task-datas task-id)
                                                                               spout-id tuple-finished-info time-delta id)
                                      Acker/ACKER_FAIL_STREAM_ID (fail-spout-msg executor-data (get task-datas task-id)
                                                                           spout-id tuple-finished-info time-delta "FAIL-STREAM" id)
                                      )))
                                ;; TODO: on failure, emit tuple to failure stream
                                ))))
        receive-queue (:receive-queue executor-data)
        event-handler (mk-task-receiver executor-data tuple-action-fn)
        has-ackers? (StormCommon/hasAckers storm-conf)
        has-eventloggers? (StormCommon/hasEventLoggers storm-conf)
        emitted-count (MutableLong. 0)
        empty-emit-streak (MutableLong. 0)
        spout-transfer-fn (fn []
                            ;; If topology was started in inactive state, don't call (.open spout) until it's activated first.
                            (while (not @(:storm-active-atom executor-data))
                              (Thread/sleep 100))
                            (log-message "Opening spout " component-id ":" (keys task-datas))
                            (.registerAll (:spout-throttling-metrics executor-data) storm-conf (.getUserContext (first (vals task-datas))))
                            (doseq [[task-id task-data] task-datas
                                    :let [^ISpout spout-obj (.getTaskObject task-data)
                                          send-spout-msg (fn [out-stream-id values message-id out-task-id]
                                                           (.increment emitted-count)
                                                           (let [out-tasks (if out-task-id
                                                                             (.getOutgoingTasks task-data out-task-id out-stream-id values)
                                                                             (.getOutgoingTasks task-data out-stream-id values))
                                                                 rooted? (and message-id has-ackers?)
                                                                 root-id (if rooted? (MessageId/generateId rand))
                                                                 ^List out-ids (fast-list-for [t out-tasks] (if rooted? (MessageId/generateId rand)))]
                                                             (fast-list-iter [out-task out-tasks id out-ids]
                                                                             (let [tuple-id (if rooted?
                                                                                              (MessageId/makeRootId root-id id)
                                                                                              (MessageId/makeUnanchored))
                                                                                   out-tuple (TupleImpl. worker-context
                                                                                                         values
                                                                                                         task-id
                                                                                                         out-stream-id
                                                                                                         tuple-id)]
                                                                               (transfer-fn out-task out-tuple)))
                                                             (if has-eventloggers?
                                                               (send-to-eventlogger executor-data task-data values component-id message-id rand))
                                                             (if (and rooted?
                                                                      (not (.isEmpty out-ids)))
                                                               (do
                                                                 (.put pending root-id [task-id
                                                                                        message-id
                                                                                        {:stream out-stream-id 
                                                                                         :values (if debug? values nil)}
                                                                                        (if (.call ^Callable sampler) (System/currentTimeMillis))])
                                                                 (send-unanchored task-data
                                                                                       Acker/ACKER_INIT_STREAM_ID
                                                                                       [root-id (Utils/bitXorVals out-ids) task-id]
                                                                                       (:transfer-fn executor-data)))
                                                               (when message-id
                                                                 (ack-spout-msg executor-data task-data message-id
                                                                                {:stream out-stream-id :values values}
                                                                                (if (.call ^Callable sampler) 0) "0:")))
                                                             (or out-tasks [])))]]

                              (.registerAll (.getBuiltInMetrics task-data) storm-conf (.getUserContext task-data))
                              (BuiltinMetricsUtil/registerQueueMetrics {"sendqueue" (:batch-transfer-queue executor-data)
                                                                       "receive" receive-queue}
                                                                      storm-conf (.getUserContext task-data))

                              (when (instance? ICredentialsListener spout-obj) (.setCredentials spout-obj initial-credentials))

                              (.open spout-obj
                                     storm-conf
                                     (.getUserContext task-data)
                                     (SpoutOutputCollector.
                                      (reify ISpoutOutputCollector
                                        (^long getPendingCount[this]
                                          (.size pending))
                                        (^List emit [this ^String stream-id ^List tuple ^Object message-id]
                                          (send-spout-msg stream-id tuple message-id nil))
                                        (^void emitDirect [this ^int out-task-id ^String stream-id
                                                           ^List tuple ^Object message-id]
                                          (send-spout-msg stream-id tuple message-id out-task-id))
                                        (reportError [this error]
                                          (report-error error))))))

                            (reset! open-or-prepare-was-called? true) 
                            (log-message "Opened spout " component-id ":" (keys task-datas))
                            (setup-metrics! executor-data)

                            (fn []
                              ;; This design requires that spouts be non-blocking
                              (.consumeBatch ^DisruptorQueue receive-queue event-handler)

                              (let [active? @(:storm-active-atom executor-data)
                                    curr-count (.get emitted-count)
                                    backpressure-enabled ((:storm-conf executor-data) TOPOLOGY-BACKPRESSURE-ENABLE)
                                    throttle-on (and backpressure-enabled
                                                     @(:throttle-on (:worker executor-data)))
                                    reached-max-spout-pending (and max-spout-pending
                                                                   (>= (.size pending) max-spout-pending))]
                                (if active?
                                        ; activated
                                  (do
                                    (when-not @last-active
                                      (reset! last-active true)
                                      (log-message "Activating spout " component-id ":" (keys task-datas))
                                      (fast-list-iter [^ISpout spout spouts] (.activate spout)))

                                    (if (and (not (.isFull transfer-queue))
                                             (not throttle-on)
                                             (not reached-max-spout-pending))
                                      (fast-list-iter [^ISpout spout spouts] (.nextTuple spout))))
                                        ; deactivated
                                  (do
                                    (when @last-active
                                      (reset! last-active false)
                                      (log-message "Deactivating spout " component-id ":" (keys task-datas))
                                      (fast-list-iter [^ISpout spout spouts] (.deactivate spout)))
                                    ;; TODO: log that it's getting throttled
                                    (Time/sleep 100)
                                    (.skippedInactive (:spout-throttling-metrics executor-data) (:stats executor-data))))

                                (if (and (= curr-count (.get emitted-count)) active?)
                                  (do (.increment empty-emit-streak)
                                      (.emptyEmit spout-wait-strategy (.get empty-emit-streak))
                                      ;; update the spout throttling metrics
                                      (if throttle-on
                                        (.skippedThrottle (:spout-throttling-metrics executor-data) (:stats executor-data))
                                        (if reached-max-spout-pending
                                          (.skippedMaxSpout (:spout-throttling-metrics executor-data) (:stats executor-data)))))
                                  (.set empty-emit-streak 0)))
                              0))]

    [(Utils/asyncLoop
      spout-transfer-fn
      false ; isDaemon
      (:report-error-and-die executor-data)
      Thread/NORM_PRIORITY
      true ; isFactory
      true ; startImmediately
      (str component-id "-executor" (:executor-id executor-data)))]))

(defn- tuple-time-delta! [^TupleImpl tuple]
  (let [ms (.getProcessSampleStartTime tuple)]
    (if ms
      (Time/deltaMs ms))))
      
(defn- tuple-execute-time-delta! [^TupleImpl tuple]
  (let [ms (.getExecuteSampleStartTime tuple)]
    (if ms
      (Time/deltaMs ms))))

(defn put-xor! [^Map pending key id]
  (let [curr (or (.get pending key) (long 0))]
    (.put pending key (bit-xor curr id))))

(defmethod mk-threads :bolt [executor-data task-datas initial-credentials]
  (let [storm-conf (:storm-conf executor-data)
        execute-sampler (ConfigUtils/mkStatsSampler storm-conf)
        executor-stats (:stats executor-data)
        {:keys [storm-conf component-id worker-context transfer-fn report-error sampler
                open-or-prepare-was-called?]} executor-data
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
                          (let [stream-id (.getSourceStreamId tuple)]
                            (condp = stream-id
                              Constants/CREDENTIALS_CHANGED_STREAM_ID 
                                (let [^Task task-data (get task-datas task-id)
                                      bolt-obj (.getTaskObject task-data)]
                                  (when (instance? ICredentialsListener bolt-obj)
                                    (.setCredentials ^ICredentialsListener bolt-obj (.getValue tuple 0))))
                              Constants/METRICS_TICK_STREAM_ID (metrics-tick executor-data (get task-datas task-id) tuple)
                              (let [^Task task-data (get task-datas task-id)
                                    ^IBolt bolt-obj (.getTaskObject task-data)
                                    user-context (.getUserContext task-data)
                                    sampler? (.call ^Callable sampler)
                                    execute-sampler? (.call ^Callable execute-sampler)
                                    now (if (or sampler? execute-sampler?) (System/currentTimeMillis))
                                    receive-queue (:receive-queue executor-data)]
                                (when sampler?
                                  (.setProcessSampleStartTime tuple now))
                                (when execute-sampler?
                                  (.setExecuteSampleStartTime tuple now))
                                (.execute bolt-obj tuple)
                                (let [delta (tuple-execute-time-delta! tuple)]
                                  (when (= true (storm-conf TOPOLOGY-DEBUG))
                                    (log-message "Execute done TUPLE " tuple " TASK: " task-id " DELTA: " delta))

                                  (.applyOn (BoltExecuteInfo. tuple task-id delta) user-context)
                                  (when delta
                                    (.boltExecuteTuple executor-stats
                                                               (.getSourceComponent tuple)
                                                               (.getSourceStreamId tuple)
                                                               delta)))))))
        has-eventloggers? (StormCommon/hasEventLoggers storm-conf)
        bolt-transfer-fn (fn []
                           ;; If topology was started in inactive state, don't call prepare bolt until it's activated first.
                           (while (not @(:storm-active-atom executor-data))
                             (Thread/sleep 100))

                           (log-message "Preparing bolt " component-id ":" (keys task-datas))
                           (doseq [[task-id task-data] task-datas
                                   :let [^IBolt bolt-obj (.getTaskObject task-data)
                                         user-context (.getUserContext task-data)
                                         transfer-fn (:transfer-fn executor-data)
                                         bolt-emit (fn [stream anchors values task]
                                                     (let [out-tasks (if task
                                                                       (.getOutgoingTasks task-data task stream values)
                                                                       (.getOutgoingTasks task-data stream values))]
                                                       (fast-list-iter [t out-tasks]
                                                                       (let [anchors-to-ids (HashMap.)]
                                                                         (fast-list-iter [^TupleImpl a anchors]
                                                                                         (let [root-ids (-> a .getMessageId .getAnchorsToIds .keySet)]
                                                                                           (when (pos? (count root-ids))
                                                                                             (let [edge-id (MessageId/generateId rand)]
                                                                                               (.updateAckVal a edge-id)
                                                                                               (fast-list-iter [root-id root-ids]
                                                                                                               (put-xor! anchors-to-ids root-id edge-id))))))
                                                                         (let [tuple (TupleImpl. worker-context
                                                                                                 values
                                                                                                 task-id
                                                                                                 stream
                                                                                                 (MessageId/makeId anchors-to-ids))]
                                                                           (transfer-fn t tuple))))
                                                       (if has-eventloggers?
                                                         (send-to-eventlogger executor-data task-data values component-id nil rand))
                                                       (or out-tasks [])))]]
                             (.registerAll (.getBuiltInMetrics task-data) storm-conf user-context)
                             (when (instance? ICredentialsListener bolt-obj) (.setCredentials bolt-obj initial-credentials))
                             (if (= component-id Constants/SYSTEM_COMPONENT_ID)
                               (do
                                 (BuiltinMetricsUtil/registerQueueMetrics {"sendqueue" (:batch-transfer-queue executor-data)
                                                                          "receive" (:receive-queue executor-data)
                                                                          "transfer" (:transfer-queue (:worker executor-data))}
                                                                         storm-conf user-context)
                                 (BuiltinMetricsUtil/registerIconnectionClientMetrics
                                   (.deref (:cached-node+port->socket (:worker executor-data))) storm-conf user-context)
                                 (BuiltinMetricsUtil/registerIconnectionServerMetric (:receiver (:worker executor-data)) storm-conf user-context))
                               (BuiltinMetricsUtil/registerQueueMetrics {"sendqueue" (:batch-transfer-queue executor-data)
                                                                        "receive" (:receive-queue executor-data)}
                                                                       storm-conf user-context))

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
                                                             (send-unanchored task-data
                                                                              Acker/ACKER_ACK_STREAM_ID
                                                                              [root (bit-xor id ack-val)]
                                                                              transfer-fn)))
                                            (let [delta (tuple-time-delta! tuple)
                                                  debug? (= true (storm-conf TOPOLOGY-DEBUG))]
                                              (when debug?
                                                (log-message "BOLT ack TASK: " task-id " TIME: " delta " TUPLE: " tuple))
                                              (.applyOn (BoltAckInfo. tuple task-id delta) user-context)
                                              (when delta
                                                (.boltAckedTuple executor-stats
                                                                         (.getSourceComponent tuple)
                                                                         (.getSourceStreamId tuple)
                                                                         delta))))
                                          (^void fail [this ^Tuple tuple]
                                            (fast-list-iter [root (.. tuple getMessageId getAnchors)]
                                                            (send-unanchored task-data
                                                                                  Acker/ACKER_FAIL_STREAM_ID
                                                                                  [root]
                                                                                  transfer-fn))
                                            (let [delta (tuple-time-delta! tuple)
                                                  debug? (= true (storm-conf TOPOLOGY-DEBUG))]
                                              (when debug?
                                                (log-message "BOLT fail TASK: " task-id " TIME: " delta " TUPLE: " tuple))
                                              (.applyOn (BoltFailInfo. tuple task-id delta) user-context)
                                              (when delta
                                                (.boltFailedTuple executor-stats
                                                                          (.getSourceComponent tuple)
                                                                          (.getSourceStreamId tuple)
                                                                          delta))))
                                          (^void resetTimeout [this ^Tuple tuple]
                                            (fast-list-iter [root (.. tuple getMessageId getAnchors)]
                                                            (send-unanchored task-data
                                                                                  Acker/ACKER_RESET_TIMEOUT_STREAM_ID
                                                                                  [root]
                                                                                  transfer-fn)))
                                          (reportError [this error]
                                            (report-error error))))))
                           (reset! open-or-prepare-was-called? true)
                           (log-message "Prepared bolt " component-id ":" (keys task-datas))
                           (setup-metrics! executor-data)

                           (let [receive-queue (:receive-queue executor-data)
                                 event-handler (mk-task-receiver executor-data tuple-action-fn)]
                             (fn []
                               (.consumeBatchWhenAvailable ^DisruptorQueue receive-queue event-handler)
                               0)))]
    ;; TODO: can get any SubscribedState objects out of the context now

    [(Utils/asyncLoop
      bolt-transfer-fn
      false ; isDaemon
      (:report-error-and-die executor-data)
      Thread/NORM_PRIORITY
      true ; isFactory
      true ; startImmediately
      (str component-id "-executor" (:executor-id executor-data)))]))

(defmethod close-component :spout [executor-data spout]
  (.close spout))

(defmethod close-component :bolt [executor-data bolt]
  (.cleanup bolt))

;; TODO: refactor this to be part of an executor-specific map
(defmethod mk-executor-stats :spout [_ rate]
  (SpoutExecutorStats. rate))

(defmethod mk-executor-stats :bolt [_ rate]
  (BoltExecutorStats. rate))
