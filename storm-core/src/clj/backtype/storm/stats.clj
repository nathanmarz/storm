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

(ns backtype.storm.stats
  (:import [backtype.storm.generated Nimbus Nimbus$Processor Nimbus$Iface StormTopology ShellComponent
            NotAliveException AlreadyAliveException InvalidTopologyException GlobalStreamId
            ClusterSummary TopologyInfo TopologySummary ExecutorInfo ExecutorSummary ExecutorStats
            ExecutorSpecificStats SpoutStats BoltStats ErrorInfo
            SupervisorSummary CommonAggregateStats ComponentAggregateStats
            ComponentPageInfo ComponentType BoltAggregateStats
            ExecutorAggregateStats SpecificAggregateStats
            SpoutAggregateStats TopologyPageInfo TopologyStats])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm.metric.internal MultiCountStatAndMetric MultiLatencyStatAndMetric])
  (:use [backtype.storm log util])
  (:use [clojure.math.numeric-tower :only [ceil]]))

(def TEN-MIN-IN-SECONDS (* 10 60))

(def COMMON-FIELDS [:emitted :transferred])
(defrecord CommonStats [^MultiCountStatAndMetric emitted
                        ^MultiCountStatAndMetric transferred
                        rate])

(def BOLT-FIELDS [:acked :failed :process-latencies :executed :execute-latencies])
;;acked and failed count individual tuples
(defrecord BoltExecutorStats [^CommonStats common
                              ^MultiCountStatAndMetric acked
                              ^MultiCountStatAndMetric failed
                              ^MultiLatencyStatAndMetric process-latencies
                              ^MultiCountStatAndMetric executed
                              ^MultiLatencyStatAndMetric execute-latencies])

(def SPOUT-FIELDS [:acked :failed :complete-latencies])
;;acked and failed count tuple completion
(defrecord SpoutExecutorStats [^CommonStats common
                               ^MultiCountStatAndMetric acked
                               ^MultiCountStatAndMetric failed
                               ^MultiLatencyStatAndMetric complete-latencies])

(def NUM-STAT-BUCKETS 20)

(defn- mk-common-stats
  [rate]
  (CommonStats.
    (MultiCountStatAndMetric. NUM-STAT-BUCKETS)
    (MultiCountStatAndMetric. NUM-STAT-BUCKETS)
    rate))

(defn mk-bolt-stats
  [rate]
  (BoltExecutorStats.
    (mk-common-stats rate)
    (MultiCountStatAndMetric. NUM-STAT-BUCKETS)
    (MultiCountStatAndMetric. NUM-STAT-BUCKETS)
    (MultiLatencyStatAndMetric. NUM-STAT-BUCKETS)
    (MultiCountStatAndMetric. NUM-STAT-BUCKETS)
    (MultiLatencyStatAndMetric. NUM-STAT-BUCKETS)))

(defn mk-spout-stats
  [rate]
  (SpoutExecutorStats.
    (mk-common-stats rate)
    (MultiCountStatAndMetric. NUM-STAT-BUCKETS)
    (MultiCountStatAndMetric. NUM-STAT-BUCKETS)
    (MultiLatencyStatAndMetric. NUM-STAT-BUCKETS)))

(defmacro stats-rate
  [stats]
  `(-> ~stats :common :rate))

(defmacro stats-emitted
  [stats]
  `(-> ~stats :common :emitted))

(defmacro stats-transferred
  [stats]
  `(-> ~stats :common :transferred))

(defmacro stats-executed
  [stats]
  `(:executed ~stats))

(defmacro stats-acked
  [stats]
  `(:acked ~stats))

(defmacro stats-failed
  [stats]
  `(:failed ~stats))

(defmacro stats-execute-latencies
  [stats]
  `(:execute-latencies ~stats))

(defmacro stats-process-latencies
  [stats]
  `(:process-latencies ~stats))

(defmacro stats-complete-latencies
  [stats]
  `(:complete-latencies ~stats))

(defn emitted-tuple!
  [stats stream]
  (.incBy ^MultiCountStatAndMetric (stats-emitted stats) ^Object stream ^long (stats-rate stats)))

(defn transferred-tuples!
  [stats stream amt]
  (.incBy ^MultiCountStatAndMetric (stats-transferred stats) ^Object stream ^long (* (stats-rate stats) amt)))

(defn bolt-execute-tuple!
  [^BoltExecutorStats stats component stream latency-ms]
  (let [key [component stream]
        ^MultiCountStatAndMetric executed (stats-executed stats)
        ^MultiLatencyStatAndMetric exec-lat (stats-execute-latencies stats)]
    (.incBy executed key (stats-rate stats))
    (.record exec-lat key latency-ms)))

(defn bolt-acked-tuple!
  [^BoltExecutorStats stats component stream latency-ms]
  (let [key [component stream]
        ^MultiCountStatAndMetric acked (stats-acked stats)
        ^MultiLatencyStatAndMetric process-lat (stats-process-latencies stats)]
    (.incBy acked key (stats-rate stats))
    (.record process-lat key latency-ms)))

(defn bolt-failed-tuple!
  [^BoltExecutorStats stats component stream latency-ms]
  (let [key [component stream]
        ^MultiCountStatAndMetric failed (stats-failed stats)]
    (.incBy failed key (stats-rate stats))))

(defn spout-acked-tuple!
  [^SpoutExecutorStats stats stream latency-ms]
  (.incBy ^MultiCountStatAndMetric (stats-acked stats) stream (stats-rate stats))
  (.record ^MultiLatencyStatAndMetric (stats-complete-latencies stats) stream latency-ms))

(defn spout-failed-tuple!
  [^SpoutExecutorStats stats stream latency-ms]
  (.incBy ^MultiCountStatAndMetric (stats-failed stats) stream (stats-rate stats)))

(defn- cleanup-stat! [stat]
  (.close stat))

(defn- cleanup-common-stats!
  [^CommonStats stats]
  (doseq [f COMMON-FIELDS]
    (cleanup-stat! (f stats))))

(defn cleanup-bolt-stats!
  [^BoltExecutorStats stats]
  (cleanup-common-stats! (:common stats))
  (doseq [f BOLT-FIELDS]
    (cleanup-stat! (f stats))))

(defn cleanup-spout-stats!
  [^SpoutExecutorStats stats]
  (cleanup-common-stats! (:common stats))
  (doseq [f SPOUT-FIELDS]
    (cleanup-stat! (f stats))))

(defn- value-stats
  [stats fields]
  (into {} (dofor [f fields]
                  [f (if (instance? MultiCountStatAndMetric (f stats))
                         (.getTimeCounts ^MultiCountStatAndMetric (f stats))
                         (.getTimeLatAvg ^MultiLatencyStatAndMetric (f stats)))])))

(defn- value-common-stats
  [^CommonStats stats]
  (merge
    (value-stats stats COMMON-FIELDS)
    {:rate (:rate stats)}))

(defn value-bolt-stats!
  [^BoltExecutorStats stats]
  (cleanup-bolt-stats! stats)
  (merge (value-common-stats (:common stats))
         (value-stats stats BOLT-FIELDS)
         {:type :bolt}))

(defn value-spout-stats!
  [^SpoutExecutorStats stats]
  (cleanup-spout-stats! stats)
  (merge (value-common-stats (:common stats))
         (value-stats stats SPOUT-FIELDS)
         {:type :spout}))

(defmulti render-stats! class-selector)

(defmethod render-stats! SpoutExecutorStats
  [stats]
  (value-spout-stats! stats))

(defmethod render-stats! BoltExecutorStats
  [stats]
  (value-bolt-stats! stats))

(defmulti thriftify-specific-stats :type)
(defmulti clojurify-specific-stats class-selector)

(defn window-set-converter
  ([stats key-fn first-key-fun]
    (into {}
      (for [[k v] stats]
        ;apply the first-key-fun only to first key.
        [(first-key-fun k)
         (into {} (for [[k2 v2] v]
                    [(key-fn k2) v2]))])))
  ([stats first-key-fun]
    (window-set-converter stats identity first-key-fun)))

(defn to-global-stream-id
  [[component stream]]
  (GlobalStreamId. component stream))

(defn from-global-stream-id [global-stream-id]
  [(.get_componentId global-stream-id) (.get_streamId global-stream-id)])

(defmethod clojurify-specific-stats BoltStats [^BoltStats stats]
  [(window-set-converter (.get_acked stats) from-global-stream-id identity)
   (window-set-converter (.get_failed stats) from-global-stream-id identity)
   (window-set-converter (.get_process_ms_avg stats) from-global-stream-id identity)
   (window-set-converter (.get_executed stats) from-global-stream-id identity)
   (window-set-converter (.get_execute_ms_avg stats) from-global-stream-id identity)])

(defmethod clojurify-specific-stats SpoutStats [^SpoutStats stats]
  [(.get_acked stats)
   (.get_failed stats)
   (.get_complete_ms_avg stats)])


(defn clojurify-executor-stats
  [^ExecutorStats stats]
  (let [ specific-stats (.get_specific stats)
         is_bolt? (.is_set_bolt specific-stats)
         specific-stats (if is_bolt? (.get_bolt specific-stats) (.get_spout specific-stats))
         specific-stats (clojurify-specific-stats specific-stats)
         common-stats (CommonStats. (.get_emitted stats)
                                    (.get_transferred stats)
                                    (.get_rate stats))]
    (if is_bolt?
      ; worker heart beat does not store the BoltExecutorStats or SpoutExecutorStats , instead it stores the result returned by render-stats!
      ; which flattens the BoltExecutorStats/SpoutExecutorStats by extracting values from all atoms and merging all values inside :common to top
      ;level map we are pretty much doing the same here.
      (dissoc (merge common-stats {:type :bolt}  (apply ->BoltExecutorStats (into [nil] specific-stats))) :common)
      (dissoc (merge common-stats {:type :spout} (apply ->SpoutExecutorStats (into [nil] specific-stats))) :common)
      )))

(defmethod thriftify-specific-stats :bolt
  [stats]
  (ExecutorSpecificStats/bolt
    (BoltStats.
      (window-set-converter (:acked stats) to-global-stream-id str)
      (window-set-converter (:failed stats) to-global-stream-id str)
      (window-set-converter (:process-latencies stats) to-global-stream-id str)
      (window-set-converter (:executed stats) to-global-stream-id str)
      (window-set-converter (:execute-latencies stats) to-global-stream-id str))))

(defmethod thriftify-specific-stats :spout
  [stats]
  (ExecutorSpecificStats/spout
    (SpoutStats. (window-set-converter (:acked stats) str)
      (window-set-converter (:failed stats) str)
      (window-set-converter (:complete-latencies stats) str))))

(defn thriftify-executor-stats
  [stats]
  (let [specific-stats (thriftify-specific-stats stats)
        rate (:rate stats)]
    (ExecutorStats. (window-set-converter (:emitted stats) str)
      (window-set-converter (:transferred stats) str)
      specific-stats
      rate)))

(defn valid-number?
  "Returns true if x is a number that is not NaN or Infinity, false otherwise"
  [x]
  (and (number? x)
       (not (Double/isNaN x))
       (not (Double/isInfinite x))))

(defn apply-default
  [f defaulting-fn & args]
  (apply f (map defaulting-fn args)))

(defn apply-or-0
  [f & args]
  (apply apply-default
         f
         #(if (valid-number? %) % 0)
         args))

(defn sum-or-0
  [& args]
  (apply apply-or-0 + args))

(defn product-or-0
  [& args]
  (apply apply-or-0 * args))

(defn max-or-0
  [& args]
  (apply apply-or-0 max args))

(defn- agg-bolt-lat-and-count
  "Aggregates number executed, process latency, and execute latency across all
  streams."
  [idk->exec-avg idk->proc-avg idk->num-executed]
  (letfn [(weight-avg [[id avg]]
            (let [num-e (get idk->num-executed id)]
              (product-or-0 avg num-e)))]
    {:executeLatencyTotal (sum (map weight-avg idk->exec-avg))
     :processLatencyTotal (sum (map weight-avg idk->proc-avg))
     :executed (sum (vals idk->num-executed))}))

(defn- agg-spout-lat-and-count
  "Aggregates number acked and complete latencies across all streams."
  [sid->comp-avg sid->num-acked]
  (letfn [(weight-avg [[id avg]]
            (product-or-0 avg (get sid->num-acked id)))]
    {:completeLatencyTotal (sum (map weight-avg sid->comp-avg))
     :acked (sum (vals sid->num-acked))}))

(defn add-pairs
  ([] [0 0])
  ([[a1 a2] [b1 b2]]
   [(+ a1 b1) (+ a2 b2)]))

(defn mk-include-sys-fn
  [include-sys?]
  (if include-sys?
    (fn [_] true)
    (fn [stream] (and (string? stream) (not (Utils/isSystemId stream))))))

(defn mk-include-sys-filter
  "Returns a function that includes or excludes map entries whose keys are
  system ids."
  [include-sys?]
  (if include-sys?
    identity
    (partial filter-key (mk-include-sys-fn false))))

(defn- agg-bolt-streams-lat-and-count
  "Aggregates number executed and process & execute latencies."
  [idk->exec-avg idk->proc-avg idk->executed]
  (letfn [(weight-avg [id avg]
            (let [num-e (idk->executed id)]
              (product-or-0 avg num-e)))]
    (into {}
      (for [k (keys idk->exec-avg)]
        [k {:executeLatencyTotal (weight-avg k (get idk->exec-avg k))
            :processLatencyTotal (weight-avg k (get idk->proc-avg k))
            :executed (idk->executed k)}]))))

(defn- agg-spout-streams-lat-and-count
  "Aggregates number acked and complete latencies."
  [idk->comp-avg idk->acked]
  (letfn [(weight-avg [id avg]
            (let [num-e (get idk->acked id)]
              (product-or-0 avg num-e)))]
    (into {}
      (for [k (keys idk->comp-avg)]
        [k {:completeLatencyTotal (weight-avg k (get idk->comp-avg k))
            :acked (get idk->acked k)}]))))

(defn swap-map-order
  "For a nested map, rearrange data such that the top-level keys become the
  nested map's keys and vice versa.
  Example:
  {:a {:X :banana, :Y :pear}, :b {:X :apple, :Y :orange}}
  -> {:Y {:a :pear, :b :orange}, :X {:a :banana, :b :apple}}"
  [m]
  (apply merge-with
         merge
         (map (fn [[k v]]
                (into {}
                      (for [[k2 v2] v]
                        [k2 {k v2}])))
              m)))

(defn- compute-agg-capacity
  "Computes the capacity metric for one executor given its heartbeat data and
  uptime."
  [m uptime]
  (when uptime
    (->>
      ;; For each stream, create weighted averages and counts.
      (merge-with (fn weighted-avg+count-fn
                    [avg cnt]
                    [(* avg cnt) cnt])
                  (get (:execute-latencies m) (str TEN-MIN-IN-SECONDS))
                  (get (:executed m) (str TEN-MIN-IN-SECONDS)))
      vals ;; Ignore the stream ids.
      (reduce add-pairs
              [0. 0]) ;; Combine weighted averages and counts.
      ((fn [[weighted-avg cnt]]
        (div weighted-avg (* 1000 (min uptime TEN-MIN-IN-SECONDS))))))))

(defn agg-pre-merge-comp-page-bolt
  [{exec-id :exec-id
    host :host
    port :port
    uptime :uptime
    comp-id :comp-id
    num-tasks :num-tasks
    statk->w->sid->num :stats}
   window
   include-sys?]
  (let [str-key (partial map-key str)
        handle-sys-components-fn (mk-include-sys-filter include-sys?)]
    {:executor-id exec-id,
     :host host,
     :port port,
     :uptime uptime,
     :num-executors 1,
     :num-tasks num-tasks,
     :capacity (compute-agg-capacity statk->w->sid->num uptime)
     :cid+sid->input-stats
     (merge-with
       merge
       (swap-map-order
         {:acked (-> statk->w->sid->num
                     :acked
                     str-key
                     (get window))
          :failed (-> statk->w->sid->num
                      :failed
                      str-key
                      (get window))})
       (agg-bolt-streams-lat-and-count (-> statk->w->sid->num
                                           :execute-latencies
                                           str-key
                                           (get window))
                                       (-> statk->w->sid->num
                                           :process-latencies
                                           str-key
                                           (get window))
                                       (-> statk->w->sid->num
                                           :executed
                                           str-key
                                           (get window)))),
     :sid->output-stats
     (swap-map-order
       {:emitted (-> statk->w->sid->num
                     :emitted
                     str-key
                     (get window)
                     handle-sys-components-fn)
        :transferred (-> statk->w->sid->num
                         :transferred
                         str-key
                         (get window)
                         handle-sys-components-fn)})}))

(defn agg-pre-merge-comp-page-spout
  [{exec-id :exec-id
    host :host
    port :port
    uptime :uptime
    comp-id :comp-id
    num-tasks :num-tasks
    statk->w->sid->num :stats}
   window
   include-sys?]
  (let [str-key (partial map-key str)
        handle-sys-components-fn (mk-include-sys-filter include-sys?)]
    {:executor-id exec-id,
     :host host,
     :port port,
     :uptime uptime,
     :num-executors 1,
     :num-tasks num-tasks,
     :sid->output-stats
     (merge-with
       merge
       (agg-spout-streams-lat-and-count (-> statk->w->sid->num
                                            :complete-latencies
                                            str-key
                                            (get window))
                                        (-> statk->w->sid->num
                                            :acked
                                            str-key
                                            (get window)))
       (swap-map-order
         {:acked (-> statk->w->sid->num
                     :acked
                     str-key
                     (get window))
          :failed (-> statk->w->sid->num
                      :failed
                      str-key
                      (get window))
          :emitted (-> statk->w->sid->num
                       :emitted
                       str-key
                       (get window)
                       handle-sys-components-fn)
          :transferred (-> statk->w->sid->num
                           :transferred
                           str-key
                           (get window)
                           handle-sys-components-fn)}))}))

(defn agg-pre-merge-topo-page-bolt
  [{comp-id :comp-id
    num-tasks :num-tasks
    statk->w->sid->num :stats
    uptime :uptime}
   window
   include-sys?]
  (let [str-key (partial map-key str)
        handle-sys-components-fn (mk-include-sys-filter include-sys?)]
    {comp-id
     (merge
       (agg-bolt-lat-and-count (-> statk->w->sid->num
                                   :execute-latencies
                                   str-key
                                   (get window))
                               (-> statk->w->sid->num
                                   :process-latencies
                                   str-key
                                   (get window))
                               (-> statk->w->sid->num
                                   :executed
                                   str-key
                                   (get window)))
       {:num-executors 1
        :num-tasks num-tasks
        :emitted (-> statk->w->sid->num
                     :emitted
                     str-key
                     (get window)
                     handle-sys-components-fn
                     vals
                     sum)
        :transferred (-> statk->w->sid->num
                         :transferred
                         str-key
                         (get window)
                         handle-sys-components-fn
                         vals
                         sum)
        :capacity (compute-agg-capacity statk->w->sid->num uptime)
        :acked (-> statk->w->sid->num
                   :acked
                   str-key
                   (get window)
                   vals
                   sum)
        :failed (-> statk->w->sid->num
                    :failed
                    str-key
                    (get window)
                    vals
                    sum)})}))

(defn agg-pre-merge-topo-page-spout
  [{comp-id :comp-id
    num-tasks :num-tasks
    statk->w->sid->num :stats}
   window
   include-sys?]
  (let [str-key (partial map-key str)
        handle-sys-components-fn (mk-include-sys-filter include-sys?)]
    {comp-id
     (merge
       (agg-spout-lat-and-count (-> statk->w->sid->num
                                    :complete-latencies
                                    str-key
                                    (get window))
                                (-> statk->w->sid->num
                                    :acked
                                    str-key
                                    (get window)))
       {:num-executors 1
        :num-tasks num-tasks
        :emitted (-> statk->w->sid->num
                     :emitted
                     str-key
                     (get window)
                     handle-sys-components-fn
                     vals
                     sum)
        :transferred (-> statk->w->sid->num
                         :transferred
                         str-key
                         (get window)
                         handle-sys-components-fn
                         vals
                         sum)
        :failed (-> statk->w->sid->num
                    :failed
                    str-key
                    (get window)
                    vals
                    sum)})}))

(defn merge-agg-comp-stats-comp-page-bolt
  [{acc-in :cid+sid->input-stats
    acc-out :sid->output-stats
    :as acc-bolt-stats}
   {bolt-in :cid+sid->input-stats
    bolt-out :sid->output-stats
    :as bolt-stats}]
  {:num-executors (inc (or (:num-executors acc-bolt-stats) 0)),
   :num-tasks (sum-or-0 (:num-tasks acc-bolt-stats) (:num-tasks bolt-stats)),
   :sid->output-stats (merge-with (partial merge-with sum-or-0)
                                  acc-out
                                  bolt-out),
   :cid+sid->input-stats (merge-with (partial merge-with sum-or-0)
                                     acc-in
                                     bolt-in),
   :executor-stats
   (let [sum-streams (fn [m k] (->> m vals (map k) (apply sum-or-0)))
         executed (sum-streams bolt-in :executed)]
     (conj (:executor-stats acc-bolt-stats)
           (merge
             (select-keys bolt-stats
                          [:executor-id :uptime :host :port :capacity])
             {:emitted (sum-streams bolt-out :emitted)
              :transferred (sum-streams bolt-out :transferred)
              :acked (sum-streams bolt-in :acked)
              :failed (sum-streams bolt-in :failed)
              :executed executed}
             (->>
               (if (and executed (pos? executed))
                 [(div (sum-streams bolt-in :executeLatencyTotal) executed)
                  (div (sum-streams bolt-in :processLatencyTotal) executed)]
                 [nil nil])
               (mapcat vector [:execute-latency :process-latency])
               (apply assoc {})))))})

(defn merge-agg-comp-stats-comp-page-spout
  [{acc-out :sid->output-stats
    :as acc-spout-stats}
   {spout-out :sid->output-stats
    :as spout-stats}]
  {:num-executors (inc (or (:num-executors acc-spout-stats) 0)),
   :num-tasks (sum-or-0 (:num-tasks acc-spout-stats) (:num-tasks spout-stats)),
   :sid->output-stats (merge-with (partial merge-with sum-or-0)
                                  acc-out
                                  spout-out),
   :executor-stats
   (let [sum-streams (fn [m k] (->> m vals (map k) (apply sum-or-0)))
         acked (sum-streams spout-out :acked)]
     (conj (:executor-stats acc-spout-stats)
           (merge
             (select-keys spout-stats [:executor-id :uptime :host :port])
             {:emitted (sum-streams spout-out :emitted)
              :transferred (sum-streams spout-out :transferred)
              :acked acked
              :failed (sum-streams spout-out :failed)}
             {:complete-latency (if (and acked (pos? acked))
                                  (div (sum-streams spout-out
                                                    :completeLatencyTotal)
                                       acked)
                                  nil)})))})

(defn merge-agg-comp-stats-topo-page-bolt
  [acc-bolt-stats bolt-stats]
  {:num-executors (inc (or (:num-executors acc-bolt-stats) 0))
   :num-tasks (sum-or-0 (:num-tasks acc-bolt-stats) (:num-tasks bolt-stats))
   :emitted (sum-or-0 (:emitted acc-bolt-stats) (:emitted bolt-stats))
   :transferred (sum-or-0 (:transferred acc-bolt-stats)
                          (:transferred bolt-stats))
   :capacity (max-or-0 (:capacity acc-bolt-stats) (:capacity bolt-stats))
   ;; We sum average latency totals here to avoid dividing at each step.
   ;; Compute the average latencies by dividing the total by the count.
   :executeLatencyTotal (sum-or-0 (:executeLatencyTotal acc-bolt-stats)
                                  (:executeLatencyTotal bolt-stats))
   :processLatencyTotal (sum-or-0 (:processLatencyTotal acc-bolt-stats)
                                  (:processLatencyTotal bolt-stats))
   :executed (sum-or-0 (:executed acc-bolt-stats) (:executed bolt-stats))
   :acked (sum-or-0 (:acked acc-bolt-stats) (:acked bolt-stats))
   :failed (sum-or-0 (:failed acc-bolt-stats) (:failed bolt-stats))})

(defn merge-agg-comp-stats-topo-page-spout
  [acc-spout-stats spout-stats]
  {:num-executors (inc (or (:num-executors acc-spout-stats) 0))
   :num-tasks (sum-or-0 (:num-tasks acc-spout-stats) (:num-tasks spout-stats))
   :emitted (sum-or-0 (:emitted acc-spout-stats) (:emitted spout-stats))
   :transferred (sum-or-0 (:transferred acc-spout-stats) (:transferred spout-stats))
   ;; We sum average latency totals here to avoid dividing at each step.
   ;; Compute the average latencies by dividing the total by the count.
   :completeLatencyTotal (sum-or-0 (:completeLatencyTotal acc-spout-stats)
                            (:completeLatencyTotal spout-stats))
   :acked (sum-or-0 (:acked acc-spout-stats) (:acked spout-stats))
   :failed (sum-or-0 (:failed acc-spout-stats) (:failed spout-stats))})

(defn aggregate-count-streams
  [stats]
  (->> stats
       (map-val #(reduce + (vals %)))))

(defn- agg-topo-exec-stats*
  "A helper function that does the common work to aggregate stats of one
  executor with the given map for the topology page."
  [window
   include-sys?
   {:keys [workers-set
           bolt-id->stats
           spout-id->stats
           window->emitted
           window->transferred
           window->comp-lat-wgt-avg
           window->acked
           window->failed] :as acc-stats}
   {:keys [stats] :as new-data}
   pre-merge-fn
   merge-fn
   comp-key]
  (let [cid->statk->num (pre-merge-fn new-data window include-sys?)
        {w->compLatWgtAvg :completeLatencyTotal
         w->acked :acked}
          (if (:complete-latencies stats)
            (swap-map-order
              (into {}
                    (for [w (keys (:acked stats))]
                         [w (agg-spout-lat-and-count
                              (get (:complete-latencies stats) w)
                              (get (:acked stats) w))])))
            {:completeLatencyTotal nil
             :acks (aggregate-count-streams (:acked stats))})
        handle-sys-components-fn (mk-include-sys-filter include-sys?)]
    (assoc {:workers-set (conj workers-set
                               [(:host new-data) (:port new-data)])
            :bolt-id->stats bolt-id->stats
            :spout-id->stats spout-id->stats
            :window->emitted (->> (:emitted stats)
                                  (map-val handle-sys-components-fn)
                                  aggregate-count-streams
                                  (merge-with + window->emitted))
            :window->transferred (->> (:transferred stats)
                                      (map-val handle-sys-components-fn)
                                      aggregate-count-streams
                                      (merge-with + window->transferred))
            :window->comp-lat-wgt-avg (merge-with +
                                                  window->comp-lat-wgt-avg
                                                  w->compLatWgtAvg)
            :window->acked (if (= :spout (:type stats))
                             (merge-with + window->acked w->acked)
                             window->acked)
            :window->failed (if (= :spout (:type stats))
                              (->> (:failed stats)
                                   aggregate-count-streams
                                   (merge-with + window->failed))
                              window->failed)}
           comp-key (merge-with merge-fn
                                (acc-stats comp-key)
                                cid->statk->num)
           :type (:type stats))))

(defmulti agg-topo-exec-stats
  "Combines the aggregate stats of one executor with the given map, selecting
  the appropriate window and including system components as specified."
  (fn dispatch-fn [& args] (:type (last args))))

(defmethod agg-topo-exec-stats :bolt
  [window include-sys? acc-stats new-data]
  (agg-topo-exec-stats* window
                        include-sys?
                        acc-stats
                        new-data
                        agg-pre-merge-topo-page-bolt
                        merge-agg-comp-stats-topo-page-bolt
                        :bolt-id->stats))

(defmethod agg-topo-exec-stats :spout
  [window include-sys? acc-stats new-data]
  (agg-topo-exec-stats* window
                        include-sys?
                        acc-stats
                        new-data
                        agg-pre-merge-topo-page-spout
                        merge-agg-comp-stats-topo-page-spout
                        :spout-id->stats))

(defmethod agg-topo-exec-stats :default [_ _ acc-stats _] acc-stats)

(defn get-last-error
  [storm-cluster-state storm-id component-id]
  (if-let [e (.last-error storm-cluster-state storm-id component-id)]
    (ErrorInfo. (:error e) (:time-secs e))))

(defn component-type
  "Returns the component type (either :bolt or :spout) for a given
  topology and component id. Returns nil if not found."
  [^StormTopology topology id]
  (let [bolts (.get_bolts topology)
        spouts (.get_spouts topology)]
    (cond
      (Utils/isSystemId id) :bolt
      (.containsKey bolts id) :bolt
      (.containsKey spouts id) :spout)))

(defn extract-nodeinfos-from-hb-for-comp
  ([exec->host+port task->component include-sys? comp-id]
   (distinct (for [[[start end :as executor] [host port]] exec->host+port
         :let [id (task->component start)]
         :when (and (or (nil? comp-id) (= comp-id id))
                 (or include-sys? (not (Utils/isSystemId id))))]
     {:host host
      :port port}))))

(defn extract-data-from-hb
  ([exec->host+port task->component beats include-sys? topology comp-id]
   (for [[[start end :as executor] [host port]] exec->host+port
         :let [beat (beats executor)
               id (task->component start)]
         :when (and (or (nil? comp-id) (= comp-id id))
                    (or include-sys? (not (Utils/isSystemId id))))]
     {:exec-id executor
      :comp-id id
      :num-tasks (count (range start (inc end)))
      :host host
      :port port
      :uptime (:uptime beat)
      :stats (:stats beat)
      :type (or (:type (:stats beat))
                (component-type topology id))}))
  ([exec->host+port task->component beats include-sys? topology]
    (extract-data-from-hb exec->host+port
                          task->component
                          beats
                          include-sys?
                          topology
                          nil)))

(defn aggregate-topo-stats
  [window include-sys? data]
  (let [init-val {:workers-set #{}
                  :bolt-id->stats {}
                  :spout-id->stats {}
                  :window->emitted {}
                  :window->transferred {}
                  :window->comp-lat-wgt-avg {}
                  :window->acked {}
                  :window->failed {}}
        reducer-fn (partial agg-topo-exec-stats
                            window
                            include-sys?)]
    (reduce reducer-fn init-val data)))

(defn- compute-weighted-averages-per-window
  [acc-data wgt-avg-key divisor-key]
  (into {} (for [[window wgt-avg] (wgt-avg-key acc-data)
                 :let [divisor ((divisor-key acc-data) window)]
                 :when (and divisor (pos? divisor))]
             [(str window) (div wgt-avg divisor)])))

(defn- post-aggregate-topo-stats
  [task->component exec->node+port last-err-fn acc-data]
  {:num-tasks (count task->component)
   :num-workers (count (:workers-set acc-data))
   :num-executors (count exec->node+port)
   :bolt-id->stats
     (into {} (for [[id m] (:bolt-id->stats acc-data)
                    :let [executed (:executed m)]]
                     [id (-> m
                             (assoc :execute-latency
                                    (if (and executed (pos? executed))
                                      (div (or (:executeLatencyTotal m) 0)
                                           executed)
                                      0)
                                    :process-latency
                                    (if (and executed (pos? executed))
                                      (div (or (:processLatencyTotal m) 0)
                                           executed)
                                      0))
                             (dissoc :executeLatencyTotal
                                     :processLatencyTotal)
                             (assoc :lastError (last-err-fn id)))]))
   :spout-id->stats
     (into {} (for [[id m] (:spout-id->stats acc-data)
                    :let [acked (:acked m)]]
                    [id (-> m
                            (assoc :complete-latency
                                   (if (and acked (pos? acked))
                                     (div (:completeLatencyTotal m)
                                          (:acked m))
                                     0))
                            (dissoc :completeLatencyTotal)
                            (assoc :lastError (last-err-fn id)))]))
   :window->emitted (map-key str (:window->emitted acc-data))
   :window->transferred (map-key str (:window->transferred acc-data))
   :window->complete-latency
     (compute-weighted-averages-per-window acc-data
                                           :window->comp-lat-wgt-avg
                                           :window->acked)
   :window->acked (map-key str (:window->acked acc-data))
   :window->failed (map-key str (:window->failed acc-data))})

(defn- thriftify-common-agg-stats
  [^ComponentAggregateStats s
   {:keys [num-tasks
           emitted
           transferred
           acked
           failed
           num-executors] :as statk->num}]
  (let [cas (CommonAggregateStats.)]
    (and num-executors (.set_num_executors cas num-executors))
    (and num-tasks (.set_num_tasks cas num-tasks))
    (and emitted (.set_emitted cas emitted))
    (and transferred (.set_transferred cas transferred))
    (and acked (.set_acked cas acked))
    (and failed (.set_failed cas failed))
    (.set_common_stats s cas)))

(defn thriftify-bolt-agg-stats
  [statk->num]
  (let [{:keys [lastError
                execute-latency
                process-latency
                executed
                capacity]} statk->num
        s (ComponentAggregateStats.)]
    (.set_type s ComponentType/BOLT)
    (and lastError (.set_last_error s lastError))
    (thriftify-common-agg-stats s statk->num)
    (.set_specific_stats s
      (SpecificAggregateStats/bolt
        (let [bas (BoltAggregateStats.)]
          (and execute-latency (.set_execute_latency_ms bas execute-latency))
          (and process-latency (.set_process_latency_ms bas process-latency))
          (and executed (.set_executed bas executed))
          (and capacity (.set_capacity bas capacity))
          bas)))
    s))

(defn thriftify-spout-agg-stats
  [statk->num]
  (let [{:keys [lastError
                complete-latency]} statk->num
        s (ComponentAggregateStats.)]
    (.set_type s ComponentType/SPOUT)
    (and lastError (.set_last_error s lastError))
    (thriftify-common-agg-stats s statk->num)
    (.set_specific_stats s
      (SpecificAggregateStats/spout
        (let [sas (SpoutAggregateStats.)]
          (and complete-latency (.set_complete_latency_ms sas complete-latency))
          sas)))
    s))

(defn thriftify-topo-page-data
  [topology-id data]
  (let [{:keys [num-tasks
                num-workers
                num-executors
                spout-id->stats
                bolt-id->stats
                window->emitted
                window->transferred
                window->complete-latency
                window->acked
                window->failed]} data
        spout-agg-stats (into {}
                              (for [[id m] spout-id->stats
                                    :let [m (assoc m :type :spout)]]
                                [id
                                 (thriftify-spout-agg-stats m)]))
        bolt-agg-stats (into {}
                             (for [[id m] bolt-id->stats
                                   :let [m (assoc m :type :bolt)]]
                              [id
                               (thriftify-bolt-agg-stats m)]))
        topology-stats (doto (TopologyStats.)
                         (.set_window_to_emitted window->emitted)
                         (.set_window_to_transferred window->transferred)
                         (.set_window_to_complete_latencies_ms
                           window->complete-latency)
                         (.set_window_to_acked window->acked)
                         (.set_window_to_failed window->failed))
      topo-page-info (doto (TopologyPageInfo. topology-id)
                       (.set_num_tasks num-tasks)
                       (.set_num_workers num-workers)
                       (.set_num_executors num-executors)
                       (.set_id_to_spout_agg_stats spout-agg-stats)
                       (.set_id_to_bolt_agg_stats bolt-agg-stats)
                       (.set_topology_stats topology-stats))]
    topo-page-info))

(defn agg-topo-execs-stats
  "Aggregate various executor statistics for a topology from the given
  heartbeats."
  [topology-id
   exec->node+port
   task->component
   beats
   topology
   window
   include-sys?
   last-err-fn]
  (->> ;; This iterates over each executor one time, because of lazy evaluation.
    (extract-data-from-hb exec->node+port
                          task->component
                          beats
                          include-sys?
                          topology)
    (aggregate-topo-stats window include-sys?)
    (post-aggregate-topo-stats task->component exec->node+port last-err-fn)
    (thriftify-topo-page-data topology-id)))

(defn- agg-bolt-exec-win-stats
  "A helper function that aggregates windowed stats from one bolt executor."
  [acc-stats new-stats include-sys?]
  (let [{w->execLatWgtAvg :executeLatencyTotal
         w->procLatWgtAvg :processLatencyTotal
         w->executed :executed}
          (swap-map-order
            (into {} (for [w (keys (:executed new-stats))]
                       [w (agg-bolt-lat-and-count
                            (get (:execute-latencies new-stats) w)
                            (get (:process-latencies new-stats) w)
                            (get (:executed new-stats) w))])))
        handle-sys-components-fn (mk-include-sys-filter include-sys?)]
    {:window->emitted (->> (:emitted new-stats)
                           (map-val handle-sys-components-fn)
                           aggregate-count-streams
                           (merge-with + (:window->emitted acc-stats)))
     :window->transferred (->> (:transferred new-stats)
                               (map-val handle-sys-components-fn)
                               aggregate-count-streams
                               (merge-with + (:window->transferred acc-stats)))
     :window->exec-lat-wgt-avg (merge-with +
                                           (:window->exec-lat-wgt-avg acc-stats)
                                           w->execLatWgtAvg)
     :window->proc-lat-wgt-avg (merge-with +
                                           (:window->proc-lat-wgt-avg acc-stats)
                                           w->procLatWgtAvg)
     :window->executed (merge-with + (:window->executed acc-stats) w->executed)
     :window->acked (->> (:acked new-stats)
                         aggregate-count-streams
                         (merge-with + (:window->acked acc-stats)))
     :window->failed (->> (:failed new-stats)
                          aggregate-count-streams
                          (merge-with + (:window->failed acc-stats)))}))

(defn- agg-spout-exec-win-stats
  "A helper function that aggregates windowed stats from one spout executor."
  [acc-stats new-stats include-sys?]
  (let [{w->compLatWgtAvg :completeLatencyTotal
         w->acked :acked}
          (swap-map-order
            (into {} (for [w (keys (:acked new-stats))]
                       [w (agg-spout-lat-and-count
                            (get (:complete-latencies new-stats) w)
                            (get (:acked new-stats) w))])))
        handle-sys-components-fn (mk-include-sys-filter include-sys?)]
    {:window->emitted (->> (:emitted new-stats)
                           (map-val handle-sys-components-fn)
                           aggregate-count-streams
                           (merge-with + (:window->emitted acc-stats)))
     :window->transferred (->> (:transferred new-stats)
                               (map-val handle-sys-components-fn)
                               aggregate-count-streams
                               (merge-with + (:window->transferred acc-stats)))
     :window->comp-lat-wgt-avg (merge-with +
                                           (:window->comp-lat-wgt-avg acc-stats)
                                           w->compLatWgtAvg)
     :window->acked (->> (:acked new-stats)
                         aggregate-count-streams
                         (merge-with + (:window->acked acc-stats)))
     :window->failed (->> (:failed new-stats)
                          aggregate-count-streams
                          (merge-with + (:window->failed acc-stats)))}))

(defmulti agg-comp-exec-stats
  "Combines the aggregate stats of one executor with the given map, selecting
  the appropriate window and including system components as specified."
  (fn dispatch-fn [_ _ init-val _] (:type init-val)))

(defmethod agg-comp-exec-stats :bolt
  [window include-sys? acc-stats new-data]
  (assoc (agg-bolt-exec-win-stats acc-stats (:stats new-data) include-sys?)
         :stats (merge-agg-comp-stats-comp-page-bolt
                  (:stats acc-stats)
                  (agg-pre-merge-comp-page-bolt new-data window include-sys?))
         :type :bolt))

(defmethod agg-comp-exec-stats :spout
  [window include-sys? acc-stats new-data]
  (assoc (agg-spout-exec-win-stats acc-stats (:stats new-data) include-sys?)
         :stats (merge-agg-comp-stats-comp-page-spout
                  (:stats acc-stats)
                  (agg-pre-merge-comp-page-spout new-data window include-sys?))
         :type :spout))

(defn- aggregate-comp-stats*
  [window include-sys? data init-val]
  (-> (partial agg-comp-exec-stats
               window
               include-sys?)
      (reduce init-val data)))

(defmulti aggregate-comp-stats
  (fn dispatch-fn [& args] (-> args last first :type)))

(defmethod aggregate-comp-stats :bolt
  [& args]
  (let [init-val {:type :bolt
                  :cid+sid->input-stats {}
                  :sid->output-stats {}
                  :executor-stats []
                  :window->emitted {}
                  :window->transferred {}
                  :window->exec-lat-wgt-avg {}
                  :window->executed {}
                  :window->proc-lat-wgt-avg {}
                  :window->acked {}
                  :window->failed {}}]
    (apply aggregate-comp-stats* (concat args (list init-val)))))

(defmethod aggregate-comp-stats :spout
  [& args]
  (let [init-val {:type :spout
                  :sid->output-stats {}
                  :executor-stats []
                  :window->emitted {}
                  :window->transferred {}
                  :window->comp-lat-wgt-avg {}
                  :window->acked {}
                  :window->failed {}}]
    (apply aggregate-comp-stats* (concat args (list init-val)))))

(defmethod aggregate-comp-stats :default [& _] {})

(defmulti post-aggregate-comp-stats
  (fn [_ _ data] (:type data)))

(defmethod post-aggregate-comp-stats :bolt
  [task->component
   exec->host+port
   {{i-stats :cid+sid->input-stats
     o-stats :sid->output-stats
     num-tasks :num-tasks
     num-executors :num-executors} :stats
    comp-type :type :as acc-data}]
  {:type comp-type
   :num-tasks num-tasks
   :num-executors num-executors
   :cid+sid->input-stats
   (->> i-stats
        (map-val (fn [m]
                     (let [executed (:executed m)
                           lats (if (and executed (pos? executed))
                                  {:execute-latency
                                   (div (or (:executeLatencyTotal m) 0)
                                        executed)
                                   :process-latency
                                   (div (or (:processLatencyTotal m) 0)
                                        executed)}
                                  {:execute-latency 0
                                   :process-latency 0})]
                       (-> m (merge lats) (dissoc :executeLatencyTotal
                                                  :processLatencyTotal))))))
   :sid->output-stats o-stats
   :executor-stats (:executor-stats (:stats acc-data))
   :window->emitted (map-key str (:window->emitted acc-data))
   :window->transferred (map-key str (:window->transferred acc-data))
   :window->execute-latency
     (compute-weighted-averages-per-window acc-data
                                           :window->exec-lat-wgt-avg
                                           :window->executed)
   :window->executed (map-key str (:window->executed acc-data))
   :window->process-latency
     (compute-weighted-averages-per-window acc-data
                                           :window->proc-lat-wgt-avg
                                           :window->executed)
   :window->acked (map-key str (:window->acked acc-data))
   :window->failed (map-key str (:window->failed acc-data))})

(defmethod post-aggregate-comp-stats :spout
  [task->component
   exec->host+port
   {{o-stats :sid->output-stats
     num-tasks :num-tasks
     num-executors :num-executors} :stats
    comp-type :type :as acc-data}]
  {:type comp-type
   :num-tasks num-tasks
   :num-executors num-executors
   :sid->output-stats
   (->> o-stats
        (map-val (fn [m]
                     (let [acked (:acked m)
                           lat (if (and acked (pos? acked))
                                 {:complete-latency
                                  (div (or (:completeLatencyTotal m) 0) acked)}
                                 {:complete-latency 0})]
                       (-> m (merge lat) (dissoc :completeLatencyTotal))))))
   :executor-stats (:executor-stats (:stats acc-data))
   :window->emitted (map-key str (:window->emitted acc-data))
   :window->transferred (map-key str (:window->transferred acc-data))
   :window->complete-latency
     (compute-weighted-averages-per-window acc-data
                                           :window->comp-lat-wgt-avg
                                           :window->acked)
   :window->acked (map-key str (:window->acked acc-data))
   :window->failed (map-key str (:window->failed acc-data))})

(defmethod post-aggregate-comp-stats :default [& _] {})

(defn thriftify-exec-agg-stats
  [comp-id comp-type {:keys [executor-id host port uptime] :as stats}]
  (doto (ExecutorAggregateStats.)
    (.set_exec_summary (ExecutorSummary. (apply #(ExecutorInfo. %1 %2)
                                                executor-id)
                                         comp-id
                                         host
                                         port
                                         (or uptime 0)))
    (.set_stats ((condp = comp-type
                   :bolt thriftify-bolt-agg-stats
                   :spout thriftify-spout-agg-stats) stats))))

(defn- thriftify-bolt-input-stats
  [cid+sid->input-stats]
  (into {} (for [[cid+sid input-stats] cid+sid->input-stats]
             [(to-global-stream-id cid+sid)
              (thriftify-bolt-agg-stats input-stats)])))

(defn- thriftify-bolt-output-stats
  [sid->output-stats]
  (map-val thriftify-bolt-agg-stats sid->output-stats))

(defn- thriftify-spout-output-stats
  [sid->output-stats]
  (map-val thriftify-spout-agg-stats sid->output-stats))

(defn thriftify-comp-page-data
  [topo-id topology comp-id data]
  (let [w->stats (swap-map-order
                   (merge
                     {:emitted (:window->emitted data)
                      :transferred (:window->transferred data)
                      :acked (:window->acked data)
                      :failed (:window->failed data)}
                     (condp = (:type data)
                       :bolt {:execute-latency (:window->execute-latency data)
                              :process-latency (:window->process-latency data)
                              :executed (:window->executed data)}
                       :spout {:complete-latency
                               (:window->complete-latency data)}
                       {}))) ; default
        [compType exec-stats w->stats gsid->input-stats sid->output-stats]
          (condp = (component-type topology comp-id)
            :bolt [ComponentType/BOLT
                   (->
                     (partial thriftify-exec-agg-stats comp-id :bolt)
                     (map (:executor-stats data)))
                   (map-val thriftify-bolt-agg-stats w->stats)
                   (thriftify-bolt-input-stats (:cid+sid->input-stats data))
                   (thriftify-bolt-output-stats (:sid->output-stats data))]
            :spout [ComponentType/SPOUT
                    (->
                      (partial thriftify-exec-agg-stats comp-id :spout)
                      (map (:executor-stats data)))
                    (map-val thriftify-spout-agg-stats w->stats)
                    nil ;; spouts do not have input stats
                    (thriftify-spout-output-stats (:sid->output-stats data))]),
        num-executors (:num-executors data)
        num-tasks (:num-tasks data)
        ret (doto (ComponentPageInfo. comp-id compType)
              (.set_topology_id topo-id)
              (.set_topology_name nil)
              (.set_window_to_stats w->stats)
              (.set_sid_to_output_stats sid->output-stats)
              (.set_exec_stats exec-stats))]
    (and num-executors (.set_num_executors ret num-executors))
    (and num-tasks (.set_num_tasks ret num-tasks))
    (and gsid->input-stats
         (.set_gsid_to_input_stats ret gsid->input-stats))
    ret))

(defn agg-comp-execs-stats
  "Aggregate various executor statistics for a component from the given
  heartbeats."
  [exec->host+port
   task->component
   beats
   window
   include-sys?
   topology-id
   topology
   component-id]
  (->> ;; This iterates over each executor one time, because of lazy evaluation.
    (extract-data-from-hb exec->host+port
                          task->component
                          beats
                          include-sys?
                          topology
                          component-id)
    (aggregate-comp-stats window include-sys?)
    (post-aggregate-comp-stats task->component exec->host+port)
    (thriftify-comp-page-data topology-id topology component-id)))

(defn expand-averages
  [avg counts]
  (let [avg (clojurify-structure avg)
        counts (clojurify-structure counts)]
    (into {}
          (for [[slice streams] counts]
            [slice
             (into {}
                   (for [[stream c] streams]
                     [stream
                      [(* c (get-in avg [slice stream]))
                       c]]
                     ))]))))

(defn expand-averages-seq
  [average-seq counts-seq]
  (->> (map vector average-seq counts-seq)
       (map #(apply expand-averages %))
       (apply merge-with (fn [s1 s2] (merge-with add-pairs s1 s2)))))

(defn- val-avg
  [[t c]]
  (if (= c 0) 0
    (double (/ t c))))

(defn aggregate-averages
  [average-seq counts-seq]
  (->> (expand-averages-seq average-seq counts-seq)
       (map-val
         (fn [s]
           (map-val val-avg s)))))

(defn aggregate-avg-streams
  [avg counts]
  (let [expanded (expand-averages avg counts)]
    (->> expanded
         (map-val #(reduce add-pairs (vals %)))
         (map-val val-avg))))

(defn pre-process
  [stream-summary include-sys?]
  (let [filter-fn (mk-include-sys-fn include-sys?)
        emitted (:emitted stream-summary)
        emitted (into {} (for [[window stat] emitted]
                           {window (filter-key filter-fn stat)}))
        transferred (:transferred stream-summary)
        transferred (into {} (for [[window stat] transferred]
                               {window (filter-key filter-fn stat)}))
        stream-summary (-> stream-summary (dissoc :emitted) (assoc :emitted emitted))
        stream-summary (-> stream-summary (dissoc :transferred) (assoc :transferred transferred))]
    stream-summary))

(defn aggregate-counts
  [counts-seq]
  (->> counts-seq
       (map clojurify-structure)
       (apply merge-with
              (fn [s1 s2]
                (merge-with + s1 s2)))))

(defn aggregate-common-stats
  [stats-seq]
  {:emitted (aggregate-counts (map #(.get_emitted ^ExecutorStats %) stats-seq))
   :transferred (aggregate-counts (map #(.get_transferred ^ExecutorStats %) stats-seq))})

(defn aggregate-bolt-stats
  [stats-seq include-sys?]
  (let [stats-seq (collectify stats-seq)]
    (merge (pre-process (aggregate-common-stats stats-seq) include-sys?)
           {:acked
            (aggregate-counts (map #(.. ^ExecutorStats % get_specific get_bolt get_acked)
                                   stats-seq))
            :failed
            (aggregate-counts (map #(.. ^ExecutorStats % get_specific get_bolt get_failed)
                                   stats-seq))
            :executed
            (aggregate-counts (map #(.. ^ExecutorStats % get_specific get_bolt get_executed)
                                   stats-seq))
            :process-latencies
            (aggregate-averages (map #(.. ^ExecutorStats % get_specific get_bolt get_process_ms_avg)
                                     stats-seq)
                                (map #(.. ^ExecutorStats % get_specific get_bolt get_acked)
                                     stats-seq))
            :execute-latencies
            (aggregate-averages (map #(.. ^ExecutorStats % get_specific get_bolt get_execute_ms_avg)
                                     stats-seq)
                                (map #(.. ^ExecutorStats % get_specific get_bolt get_executed)
                                     stats-seq))})))

(defn aggregate-spout-stats
  [stats-seq include-sys?]
  (let [stats-seq (collectify stats-seq)]
    (merge (pre-process (aggregate-common-stats stats-seq) include-sys?)
           {:acked
            (aggregate-counts (map #(.. ^ExecutorStats % get_specific get_spout get_acked)
                                   stats-seq))
            :failed
            (aggregate-counts (map #(.. ^ExecutorStats % get_specific get_spout get_failed)
                                   stats-seq))
            :complete-latencies
            (aggregate-averages (map #(.. ^ExecutorStats % get_specific get_spout get_complete_ms_avg)
                                     stats-seq)
                                (map #(.. ^ExecutorStats % get_specific get_spout get_acked)
                                     stats-seq))})))

(defn get-filled-stats
  [summs]
  (->> summs
       (map #(.get_stats ^ExecutorSummary %))
       (filter not-nil?)))

(defn aggregate-spout-streams
  [stats]
  {:acked (aggregate-count-streams (:acked stats))
   :failed (aggregate-count-streams (:failed stats))
   :emitted (aggregate-count-streams (:emitted stats))
   :transferred (aggregate-count-streams (:transferred stats))
   :complete-latencies (aggregate-avg-streams (:complete-latencies stats)
                                              (:acked stats))})

(defn spout-streams-stats
  [summs include-sys?]
  (let [stats-seq (get-filled-stats summs)]
    (aggregate-spout-streams
      (aggregate-spout-stats
        stats-seq include-sys?))))

(defn aggregate-bolt-streams
  [stats]
  {:acked (aggregate-count-streams (:acked stats))
   :failed (aggregate-count-streams (:failed stats))
   :emitted (aggregate-count-streams (:emitted stats))
   :transferred (aggregate-count-streams (:transferred stats))
   :process-latencies (aggregate-avg-streams (:process-latencies stats)
                                             (:acked stats))
   :executed (aggregate-count-streams (:executed stats))
   :execute-latencies (aggregate-avg-streams (:execute-latencies stats)
                                             (:executed stats))})

(defn compute-executor-capacity
  [^ExecutorSummary e]
  (let [stats (.get_stats e)
        stats (if stats
                (-> stats
                    (aggregate-bolt-stats true)
                    (aggregate-bolt-streams)
                    swap-map-order
                    (get (str TEN-MIN-IN-SECONDS))))
        uptime (nil-to-zero (.get_uptime_secs e))
        window (if (< uptime TEN-MIN-IN-SECONDS) uptime TEN-MIN-IN-SECONDS)
        executed (-> stats :executed nil-to-zero)
        latency (-> stats :execute-latencies nil-to-zero)]
    (if (> window 0)
      (div (* executed latency) (* 1000 window)))))

(defn bolt-streams-stats
  [summs include-sys?]
  (let [stats-seq (get-filled-stats summs)]
    (aggregate-bolt-streams
      (aggregate-bolt-stats
        stats-seq include-sys?))))

(defn total-aggregate-stats
  [spout-summs bolt-summs include-sys?]
  (let [spout-stats (get-filled-stats spout-summs)
        bolt-stats (get-filled-stats bolt-summs)
        agg-spout-stats (-> spout-stats
                            (aggregate-spout-stats include-sys?)
                            aggregate-spout-streams)
        agg-bolt-stats (-> bolt-stats
                           (aggregate-bolt-stats include-sys?)
                           aggregate-bolt-streams)]
    (merge-with
      (fn [s1 s2]
        (merge-with + s1 s2))
      (select-keys
        agg-bolt-stats
        ;; Include only keys that will be used.  We want to count acked and
        ;; failed only for the "tuple trees," so we do not include those keys
        ;; from the bolt executors.
        [:emitted :transferred])
      agg-spout-stats)))

(defn error-subset
  [error-str]
  (apply str (take 200 error-str)))

(defn most-recent-error
  [errors-list]
  (let [error (->> errors-list
                   (sort-by #(.get_error_time_secs ^ErrorInfo %))
                   reverse
                   first)]
    (if error
      (error-subset (.get_error ^ErrorInfo error))
      "")))

(defn float-str [n]
  (if n
    (format "%.3f" (float n))
    "0"))

(defn compute-bolt-capacity
  [executors]
  (->> executors
       (map compute-executor-capacity)
       (map nil-to-zero)
       (apply max)))
