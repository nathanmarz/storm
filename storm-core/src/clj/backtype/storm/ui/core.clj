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

(ns backtype.storm.ui.core
  (:use compojure.core)
  (:use ring.middleware.reload)
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util log])
  (:use [backtype.storm.ui helpers])
  (:use [backtype.storm.daemon [common :only [ACKER-COMPONENT-ID ACKER-INIT-STREAM-ID
                                              ACKER-ACK-STREAM-ID ACKER-FAIL-STREAM-ID system-id?]]])
  (:use [ring.adapter.jetty :only [run-jetty]])
  (:use [clojure.string :only [trim]])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm.generated ExecutorSpecificStats
            ExecutorStats ExecutorSummary TopologyInfo SpoutStats BoltStats
            ErrorInfo ClusterSummary SupervisorSummary TopologySummary
            Nimbus$Client StormTopology GlobalStreamId RebalanceOptions
            KillOptions])
  (:import [java.io File])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.util.response :as resp]
            [backtype.storm [thrift :as thrift]])
  (:import [org.apache.commons.lang StringEscapeUtils])
  (:gen-class))

(def ^:dynamic *STORM-CONF* (read-storm-config))

(defmacro with-nimbus
  [nimbus-sym & body]
  `(thrift/with-nimbus-connection
     [~nimbus-sym (*STORM-CONF* NIMBUS-HOST) (*STORM-CONF* NIMBUS-THRIFT-PORT)]
     ~@body))

(defn get-filled-stats
  [summs]
  (->> summs
       (map #(.get_stats ^ExecutorSummary %))
       (filter not-nil?)))

(defn read-storm-version
  "Returns a string containing the Storm version or 'Unknown'."
  []
  (let [storm-home (System/getProperty "storm.home")
        release-path (format "%s/RELEASE" storm-home)
        release-file (File. release-path)]
    (if (and (.exists release-file) (.isFile release-file))
      (trim (slurp release-path))
      "Unknown")))

(defn component-type
  "Returns the component type (either :bolt or :spout) for a given
  topology and component id. Returns nil if not found."
  [^StormTopology topology id]
  (let [bolts (.get_bolts topology)
        spouts (.get_spouts topology)]
    (cond
      (.containsKey bolts id) :bolt
      (.containsKey spouts id) :spout)))

(defn executor-summary-type
  [topology ^ExecutorSummary s]
  (component-type topology (.get_component_id s)))

(defn add-pairs
  ([] [0 0])
  ([[a1 a2] [b1 b2]]
   [(+ a1 b1) (+ a2 b2)]))

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
  (if (= t 0) 0
    (double (/ t c))))

(defn aggregate-averages
  [average-seq counts-seq]
  (->> (expand-averages-seq average-seq counts-seq)
       (map-val
         (fn [s]
           (map-val val-avg s)))))

(defn aggregate-counts
  [counts-seq]
  (->> counts-seq
       (map clojurify-structure)
       (apply merge-with
              (fn [s1 s2]
                (merge-with + s1 s2)))))

(defn aggregate-avg-streams
  [avg counts]
  (let [expanded (expand-averages avg counts)]
    (->> expanded
         (map-val #(reduce add-pairs (vals %)))
         (map-val val-avg))))

(defn aggregate-count-streams
  [stats]
  (->> stats
       (map-val #(reduce + (vals %)))))

(defn aggregate-common-stats
  [stats-seq]
  {:emitted (aggregate-counts (map #(.get_emitted ^ExecutorStats %) stats-seq))
   :transferred (aggregate-counts (map #(.get_transferred ^ExecutorStats %) stats-seq))})

(defn mk-include-sys-fn
  [include-sys?]
  (if include-sys?
    (fn [_] true)
    (fn [stream] (and (string? stream) (not (system-id? stream))))))

(defn is-ack-stream
  [stream]
  (let [acker-streams
        [ACKER-INIT-STREAM-ID
         ACKER-ACK-STREAM-ID
         ACKER-FAIL-STREAM-ID]]
    (every? #(not= %1 stream) acker-streams)))

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

(defn aggregate-spout-streams
  [stats]
  {:acked (aggregate-count-streams (:acked stats))
   :failed (aggregate-count-streams (:failed stats))
   :emitted (aggregate-count-streams (:emitted stats))
   :transferred (aggregate-count-streams (:transferred stats))
   :complete-latencies (aggregate-avg-streams (:complete-latencies stats)
                                              (:acked stats))})

(defn spout-summary?
  [topology s]
  (= :spout (executor-summary-type topology s)))

(defn bolt-summary?
  [topology s]
  (= :bolt (executor-summary-type topology s)))

(defn group-by-comp
  [summs]
  (let [ret (group-by #(.get_component_id ^ExecutorSummary %) summs)]
    (into (sorted-map) ret )))

(defn error-subset
  [error-str]
  (apply str (take 200 error-str)))

(defn most-recent-error
  [errors-list]
  (let [error (->> errors-list
                   (sort-by #(.get_error_time_secs ^ErrorInfo %))
                   reverse
                   first)]
    error))

(defn component-task-summs
  [^TopologyInfo summ topology id]
  (let [spout-summs (filter (partial spout-summary? topology) (.get_executors summ))
        bolt-summs (filter (partial bolt-summary? topology) (.get_executors summ))
        spout-comp-summs (group-by-comp spout-summs)
        bolt-comp-summs (group-by-comp bolt-summs)
        ret (if (contains? spout-comp-summs id)
              (spout-comp-summs id)
              (bolt-comp-summs id))]
    (sort-by #(-> ^ExecutorSummary % .get_executor_info .get_task_start) ret)))

(defn worker-log-link [host port]
  (url-format "http://%s:%s/log?file=worker-%s.log"
              host (*STORM-CONF* LOGVIEWER-PORT) port))

(defn compute-executor-capacity
  [^ExecutorSummary e]
  (let [stats (.get_stats e)
        stats (if stats
                (-> stats
                    (aggregate-bolt-stats true)
                    (aggregate-bolt-streams)
                    swap-map-order
                    (get "600")))
        uptime (nil-to-zero (.get_uptime_secs e))
        window (if (< uptime 600) uptime 600)
        executed (-> stats :executed nil-to-zero)
        latency (-> stats :execute-latencies nil-to-zero)]
    (if (> window 0)
      (div (* executed latency) (* 1000 window)))))

(defn compute-bolt-capacity
  [executors]
  (->> executors
       (map compute-executor-capacity)
       (map nil-to-zero)
       (apply max)))

(defn get-error-time
  [error]
  (if error
    (time-delta (.get_error_time_secs ^ErrorInfo error))))

(defn get-error-data 
  [error]
  (if error
    (error-subset (.get_error ^ErrorInfo error))
    ""))

(defn get-error-port
  [error error-host top-id]
  (if error
    (.get_port ^ErrorInfo error)
    ""))

(defn get-error-host
  [error]
  (if error
    (.get_host ^ErrorInfo error)
    ""))

(defn spout-streams-stats
  [summs include-sys?]
  (let [stats-seq (get-filled-stats summs)]
    (aggregate-spout-streams
      (aggregate-spout-stats
        stats-seq include-sys?))))

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
        [:emitted :transferred :acked :failed :complete-latencies])
      (select-keys
        agg-spout-stats
        [:emitted :transferred :acked :failed :complete-latencies]))))

(defn stats-times
  [stats-map]
  (sort-by #(Integer/parseInt %)
           (-> stats-map
               clojurify-structure
               (dissoc ":all-time")
               keys)))

(defn window-hint
  [window]
  (if (= window ":all-time")
    "All time"
    (pretty-uptime-sec window)))

(defn topology-action-button
  [id name action command is-wait default-wait enabled]
  [:input {:type "button"
           :value action
           (if enabled :enabled :disabled) ""
           :onclick (str "confirmAction('"
                         (StringEscapeUtils/escapeJavaScript id) "', '"
                         (StringEscapeUtils/escapeJavaScript name) "', '"
                         command "', " is-wait ", " default-wait ")")}])

(defn sanitize-stream-name
  [name]
  (let [sym-regex #"(?![A-Za-z_\-:\.])."]
    (str
     (if (re-find #"^[A-Za-z]" name)
       (clojure.string/replace name sym-regex "_")
       (clojure.string/replace (str \s name) sym-regex "_"))
     (hash name))))

(defn sanitize-transferred
  [transferred]
  (into {}
        (for [[time, stream-map] transferred]
          [time, (into {}
                       (for [[stream, trans] stream-map]
                         [(sanitize-stream-name stream), trans]))])))

(defn visualization-data
  [spout-bolt spout-comp-summs bolt-comp-summs window storm-id]
  (let [components (for [[id spec] spout-bolt]
            [id
             (let [inputs (.get_inputs (.get_common spec))
                   bolt-summs (get bolt-comp-summs id)
                   spout-summs (get spout-comp-summs id)
                   bolt-cap (if bolt-summs
                              (compute-bolt-capacity bolt-summs)
                              0)]
               {:type (if bolt-summs "bolt" "spout")
                :capacity bolt-cap
                :latency (if bolt-summs
                           (get-in
                             (bolt-streams-stats bolt-summs true)
                             [:process-latencies window])
                           (get-in
                             (spout-streams-stats spout-summs true)
                             [:complete-latencies window]))
                :transferred (or
                               (get-in
                                 (spout-streams-stats spout-summs true)
                                 [:transferred window])
                               (get-in
                                 (bolt-streams-stats bolt-summs true)
                                 [:transferred window]))
                :stats (let [mapfn (fn [dat]
                                     (map (fn [^ExecutorSummary summ]
                                            {:host (.get_host summ)
                                             :port (.get_port summ)
                                             :uptime_secs (.get_uptime_secs summ)
                                             :transferred (if-let [stats (.get_stats summ)]
                                                            (sanitize-transferred (.get_transferred stats)))})
                                          dat))]
                         (if bolt-summs
                           (mapfn bolt-summs)
                           (mapfn spout-summs)))
                :link (url-format "/component.html?id=%s&topology_id=%s" id storm-id)
                :inputs (for [[global-stream-id group] inputs]
                          {:component (.get_componentId global-stream-id)
                           :stream (.get_streamId global-stream-id)
                           :sani-stream (sanitize-stream-name (.get_streamId global-stream-id))
                           :grouping (clojure.core/name (thrift/grouping-type group))})})])]
    (into {} (doall components))))

(defn stream-boxes [datmap]
  (let [filter-fn (mk-include-sys-fn true)
        streams
        (vec (doall (distinct
                     (apply concat
                            (for [[k v] datmap]
                              (for [m (get v :inputs)]
                                {:stream (get m :stream)
                                 :sani-stream (get m :sani-stream)
                                 :checked (is-ack-stream (get m :stream))}))))))]
    (map (fn [row]
           {:row row}) (partition 4 4 nil streams))))

(defn mk-visualization-data [id window include-sys?]
  (with-nimbus
    nimbus
    (let [window (if window window ":all-time")
          topology (.getTopology ^Nimbus$Client nimbus id)
          spouts (.get_spouts topology)
          bolts (.get_bolts topology)
          summ (.getTopologyInfo ^Nimbus$Client nimbus id)
          execs (.get_executors summ)
          spout-summs (filter (partial spout-summary? topology) execs)
          bolt-summs (filter (partial bolt-summary? topology) execs)
          spout-comp-summs (group-by-comp spout-summs)
          bolt-comp-summs (group-by-comp bolt-summs)
          bolt-comp-summs (filter-key (mk-include-sys-fn include-sys?)
                                      bolt-comp-summs)
          topology-conf (from-json
                          (.getTopologyConf ^Nimbus$Client nimbus id))]
      (visualization-data
        (merge (hashmap-to-persistent spouts)
               (hashmap-to-persistent bolts))
        spout-comp-summs bolt-comp-summs window id))))

(defn cluster-configuration []
  (with-nimbus nimbus
    (.getNimbusConf ^Nimbus$Client nimbus)))

(defn cluster-summary
  ([]
     (with-nimbus nimbus
        (cluster-summary (.getClusterInfo ^Nimbus$Client nimbus))))
  ([^ClusterSummary summ]
     (let [sups (.get_supervisors summ)
        used-slots (reduce + (map #(.get_num_used_workers ^SupervisorSummary %) sups))
        total-slots (reduce + (map #(.get_num_workers ^SupervisorSummary %) sups))
        free-slots (- total-slots used-slots)
        total-tasks (->> (.get_topologies summ)
                         (map #(.get_num_tasks ^TopologySummary %))
                         (reduce +))
        total-executors (->> (.get_topologies summ)
                             (map #(.get_num_executors ^TopologySummary %))
                             (reduce +))]
       {"stormVersion" (read-storm-version)
        "nimbusUptime" (pretty-uptime-sec (.get_nimbus_uptime_secs summ))
        "supervisors" (count sups)
        "slotsTotal" total-slots
        "slotsUsed"  used-slots
        "slotsFree" free-slots
        "executorsTotal" total-executors
        "tasksTotal" total-tasks})))

(defn supervisor-summary
  ([]
   (with-nimbus nimbus
                (supervisor-summary
                  (.get_supervisors (.getClusterInfo ^Nimbus$Client nimbus)))))
  ([summs]
   {"supervisors"
    (for [^SupervisorSummary s summs]
      {"id" (.get_supervisor_id s)
       "host" (.get_host s)
       "uptime" (pretty-uptime-sec (.get_uptime_secs s))
       "slotsTotal" (.get_num_workers s)
       "slotsUsed" (.get_num_used_workers s)})}))

(defn all-topologies-summary
  ([]
   (with-nimbus
     nimbus
     (all-topologies-summary
       (.get_topologies (.getClusterInfo ^Nimbus$Client nimbus)))))
  ([summs]
   {"topologies"
    (for [^TopologySummary t summs]
      {"id" (.get_id t)
       "name" (.get_name t)
       "status" (.get_status t)
       "uptime" (pretty-uptime-sec (.get_uptime_secs t))
       "tasksTotal" (.get_num_tasks t)
       "workersTotal" (.get_num_workers t)
       "executorsTotal" (.get_num_executors t)})}))

(defn topology-stats [id window stats]
  (let [times (stats-times (:emitted stats))
        display-map (into {} (for [t times] [t pretty-uptime-sec]))
        display-map (assoc display-map ":all-time" (fn [_] "All time"))]
    (for [k (concat times [":all-time"])
          :let [disp ((display-map k) k)]]
      {"windowPretty" disp
       "window" k
       "emitted" (get-in stats [:emitted k])
       "transferred" (get-in stats [:transferred k])
       "completeLatency" (float-str (get-in stats [:complete-latencies k]))
       "acked" (get-in stats [:acked k])
       "failed" (get-in stats [:failed k])})))

(defn spout-comp [top-id summ-map errors window include-sys?]
  (for [[id summs] summ-map
        :let [stats-seq (get-filled-stats summs)
              stats (aggregate-spout-streams
                     (aggregate-spout-stats
                      stats-seq include-sys?))
              last-error (most-recent-error (get errors id))
              error-host (get-error-host last-error)
              error-port (get-error-port last-error error-host top-id) ]]
    {"spoutId" id
     "executors" (count summs)
     "tasks" (sum-tasks summs)
     "emitted" (get-in stats [:emitted window])
     "transferred" (get-in stats [:transferred window])
     "completeLatency" (float-str (get-in stats [:complete-latencies window]))
     "acked" (get-in stats [:acked window])
     "failed" (get-in stats [:failed window])
     "errorHost" error-host
     "errorPort" error-port
     "errorWorkerLogLink" (worker-log-link error-host error-port)
     "errorLapsedSecs" (get-error-time last-error)
     "lastError" (get-error-data last-error) }))

(defn bolt-comp [top-id summ-map errors window include-sys?]
  (for [[id summs] summ-map
        :let [stats-seq (get-filled-stats summs)
              stats (aggregate-bolt-streams
                     (aggregate-bolt-stats
                      stats-seq include-sys?))
              last-error (most-recent-error (get errors id))
              error-host (get-error-host last-error)
              error-port (get-error-port last-error error-host top-id) ]]
    {"boltId" id
     "executors" (count summs)
     "tasks" (sum-tasks summs)
     "emitted" (get-in stats [:emitted window])
     "transferred" (get-in stats [:transferred window])
     "capacity" (float-str (nil-to-zero (compute-bolt-capacity summs)))
     "executeLatency" (float-str (get-in stats [:execute-latencies window]))
     "executed" (get-in stats [:executed window])
     "processLatency" (float-str (get-in stats [:process-latencies window]))
     "acked" (get-in stats [:acked window])
     "failed" (get-in stats [:failed window])
     "errorHost" error-host
     "errorPort" error-port
     "errorWorkerLogLink" (worker-log-link error-host error-port)
     "errorLapsedSecs" (get-error-time last-error)
     "lastError" (get-error-data last-error) }))

(defn topology-summary [^TopologyInfo summ]
  (let [executors (.get_executors summ)
        workers (set (for [^ExecutorSummary e executors]
                       [(.get_host e) (.get_port e)]))]
      {"id" (.get_id summ)
       "name" (.get_name summ)
       "status" (.get_status summ)
       "uptime" (pretty-uptime-sec (.get_uptime_secs summ))
       "tasksTotal" (sum-tasks executors)
       "workersTotal" (count workers)
       "executorsTotal" (count executors)}))

(defn spout-summary-json [topology-id id stats window]
  (let [times (stats-times (:emitted stats))
        display-map (into {} (for [t times] [t pretty-uptime-sec]))
        display-map (assoc display-map ":all-time" (fn [_] "All time"))]
     (for [k (concat times [":all-time"])
           :let [disp ((display-map k) k)]]
       {"windowPretty" disp
        "window" k
        "emitted" (get-in stats [:emitted k])
        "transferred" (get-in stats [:transferred k])
        "completeLatency" (float-str (get-in stats [:complete-latencies k]))
        "acked" (get-in stats [:acked k])
        "failed" (get-in stats [:failed k])})))

(defn topology-page [id window include-sys?]
  (with-nimbus nimbus
    (let [window (if window window ":all-time")
          window-hint (window-hint window)
          summ (.getTopologyInfo ^Nimbus$Client nimbus id)
          topology (.getTopology ^Nimbus$Client nimbus id)
          topology-conf (from-json (.getTopologyConf ^Nimbus$Client nimbus id))
          spout-summs (filter (partial spout-summary? topology) (.get_executors summ))
          bolt-summs (filter (partial bolt-summary? topology) (.get_executors summ))
          spout-comp-summs (group-by-comp spout-summs)
          bolt-comp-summs (group-by-comp bolt-summs)
          bolt-comp-summs (filter-key (mk-include-sys-fn include-sys?) bolt-comp-summs)
          name (.get_name summ)
          status (.get_status summ)
          msg-timeout (topology-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS)
          spouts (.get_spouts topology)
          bolts (.get_bolts topology)
          visualizer-data (visualization-data (merge (hashmap-to-persistent spouts)
                                                     (hashmap-to-persistent bolts))
                                              spout-comp-summs
                                              bolt-comp-summs
                                              window
                                              id)]
      (merge
       (topology-summary summ)
       {"window" window
        "windowHint" window-hint
        "msgTimeout" msg-timeout
        "topologyStats" (topology-stats id window (total-aggregate-stats spout-summs bolt-summs include-sys?))
        "spouts" (spout-comp id spout-comp-summs (.get_errors summ) window include-sys?)
        "bolts" (bolt-comp id bolt-comp-summs (.get_errors summ) window include-sys?)
        "configuration" topology-conf
        "visualizationTable" (stream-boxes visualizer-data)}))))

(defn spout-output-stats
  [stream-summary window]
  (let [stream-summary (map-val swap-map-order (swap-map-order stream-summary))]
    (for [[s stats] (stream-summary window)]
      {"stream" s
       "emitted" (nil-to-zero (:emitted stats))
       "transferred" (nil-to-zero (:transferred stats))
       "completeLatency" (float-str (:complete-latencies stats))
       "acked" (nil-to-zero (:acked stats))
       "failed" (nil-to-zero (:failed stats))})))

(defn spout-executor-stats
  [topology-id executors window include-sys?]
  (for [^ExecutorSummary e executors
        :let [stats (.get_stats e)
              stats (if stats
                      (-> stats
                          (aggregate-spout-stats include-sys?)
                          aggregate-spout-streams
                          swap-map-order
                          (get window)))]]
    {"id" (pretty-executor-info (.get_executor_info e))
     "uptime" (pretty-uptime-sec (.get_uptime_secs e))
     "host" (.get_host e)
     "port" (.get_port e)
     "emitted" (nil-to-zero (:emitted stats))
     "transferred" (nil-to-zero (:transferred stats))
     "completeLatency" (float-str (:complete-latencies stats))
     "acked" (nil-to-zero (:acked stats))
     "failed" (nil-to-zero (:failed stats))
     "workerLogLink" (worker-log-link (.get_host e) (.get_port e))}))

(defn component-errors
  [errors-list topology-id]
  (let [errors (->> errors-list
                    (sort-by #(.get_error_time_secs ^ErrorInfo %))
                    reverse)]
    {"componentErrors"
     (for [^ErrorInfo e errors]
       {"time" (* 1000 (long (.get_error_time_secs e)))
        "errorHost" (.get_host e)
        "errorPort"  (.get_port e)
        "errorWorkerLogLink"  (worker-log-link (.get_host e) (.get_port e))
        "errorLapsedSecs" (get-error-time e)
        "error" (.get_error e)})}))

(defn spout-stats
  [window ^TopologyInfo topology-info component executors include-sys?]
  (let [window-hint (str " (" (window-hint window) ")")
        stats (get-filled-stats executors)
        stream-summary (-> stats (aggregate-spout-stats include-sys?))
        summary (-> stream-summary aggregate-spout-streams)]
    {"spoutSummary" (spout-summary-json
                      (.get_id topology-info) component summary window)
     "outputStats" (spout-output-stats stream-summary window)
     "executorStats" (spout-executor-stats (.get_id topology-info)
                                           executors window include-sys?)}))

(defn bolt-summary
  [topology-id id stats window]
  (let [times (stats-times (:emitted stats))
        display-map (into {} (for [t times] [t pretty-uptime-sec]))
        display-map (assoc display-map ":all-time" (fn [_] "All time"))]
    (for [k (concat times [":all-time"])
          :let [disp ((display-map k) k)]]
      {"window" k
       "windowPretty" disp
       "emitted" (get-in stats [:emitted k])
       "transferred" (get-in stats [:transferred k])
       "executeLatency" (float-str (get-in stats [:execute-latencies k]))
       "executed" (get-in stats [:executed k])
       "processLatency" (float-str (get-in stats [:process-latencies k]))
       "acked" (get-in stats [:acked k])
       "failed" (get-in stats [:failed k])})))

(defn bolt-output-stats
  [stream-summary window]
  (let [stream-summary (-> stream-summary
                           swap-map-order
                           (get window)
                           (select-keys [:emitted :transferred])
                           swap-map-order)]
    (for [[s stats] stream-summary]
      {"stream" s
        "emitted" (nil-to-zero (:emitted stats))
        "transferred" (nil-to-zero (:transferred stats))})))

(defn bolt-input-stats
  [stream-summary window]
  (let [stream-summary
        (-> stream-summary
            swap-map-order
            (get window)
            (select-keys [:acked :failed :process-latencies
                          :executed :execute-latencies])
            swap-map-order)]
    (for [[^GlobalStreamId s stats] stream-summary]
      {"component" (.get_componentId s)
       "stream" (.get_streamId s)
       "executeLatency" (float-str (:execute-latencies stats))
       "processLatency" (float-str (:execute-latencies stats))
       "executed" (nil-to-zero (:executed stats))
       "acked" (nil-to-zero (:acked stats))
       "failed" (nil-to-zero (:failed stats))})))

(defn bolt-executor-stats
  [topology-id executors window include-sys?]
  (for [^ExecutorSummary e executors
        :let [stats (.get_stats e)
              stats (if stats
                      (-> stats
                          (aggregate-bolt-stats include-sys?)
                          (aggregate-bolt-streams)
                          swap-map-order
                          (get window)))]]
    {"id" (pretty-executor-info (.get_executor_info e))
     "uptime" (pretty-uptime-sec (.get_uptime_secs e))
     "host" (.get_host e)
     "port" (.get_port e)
     "emitted" (nil-to-zero (:emitted stats))
     "transferred" (nil-to-zero (:transferred stats))
     "capacity" (float-str (nil-to-zero (compute-executor-capacity e)))
     "executeLatency" (float-str (:execute-latencies stats))
     "executed" (nil-to-zero (:executed stats))
     "processLatency" (float-str (:process-latencies stats))
     "acked" (nil-to-zero (:acked stats))
     "failed" (nil-to-zero (:failed stats))
     "workerLogLink" (worker-log-link (.get_host e) (.get_port e))}))

(defn bolt-stats
  [window ^TopologyInfo topology-info component executors include-sys?]
  (let [window-hint (str " (" (window-hint window) ")")
        stats (get-filled-stats executors)
        stream-summary (-> stats (aggregate-bolt-stats include-sys?))
        summary (-> stream-summary aggregate-bolt-streams)]
    {"boltStats" (bolt-summary (.get_id topology-info) component summary window)
     "inputStats" (bolt-input-stats stream-summary window)
     "outputStats" (bolt-output-stats stream-summary window)
     "executorStats" (bolt-executor-stats
                       (.get_id topology-info) executors window include-sys?)}))

(defn component-page
  [topology-id component window include-sys?]
  (with-nimbus nimbus
    (let [window (if window window ":all-time")
          summ (.getTopologyInfo ^Nimbus$Client nimbus topology-id)
          topology (.getTopology ^Nimbus$Client nimbus topology-id)
          type (component-type topology component)
          summs (component-task-summs summ topology component)
          spec (cond (= type :spout) (spout-stats window summ component summs include-sys?)
                     (= type :bolt) (bolt-stats window summ component summs include-sys?))
          errors (component-errors (get (.get_errors summ) component) topology-id)]
      (merge
       {"id" component
         "name" (.get_name summ)
         "executors" (count summs)
         "tasks" (sum-tasks summs)
         "topologyId" topology-id
         "window" window
         "componentType" (name type)
         "windowHint" (window-hint window)}
       spec errors))))

(defn check-include-sys?
  [sys?]
  (if (or (nil? sys?) (= "false" sys?)) false true))

(defn json-response [data & [status]]
  {:status (or status 200)
   :headers {"Content-Type" "application/json"}
   :body (to-json data)})

(defroutes main-routes
  (GET "/api/v1/cluster/configuration" []
       (cluster-configuration))
  (GET "/api/v1/cluster/summary" []
       (json-response (cluster-summary)))
  (GET "/api/v1/supervisor/summary" []
       (json-response (supervisor-summary)))
  (GET "/api/v1/topology/summary" []
       (json-response (all-topologies-summary)))
  (GET  "/api/v1/topology/:id" [id & m]
        (let [id (url-decode id)]
          (json-response (topology-page id (:window m) (check-include-sys? (:sys m))))))
  (GET "/api/v1/topology/:id/visualization" [:as {:keys [cookies servlet-request]} id & m]
       (json-response (mk-visualization-data id (:window m) (check-include-sys? (:sys m)))))
  (GET "/api/v1/topology/:id/component/:component" [id component & m]
       (let [id (url-decode id)
             component (url-decode component)]
         (json-response (component-page id component (:window m) (check-include-sys? (:sys m))))))
  (POST "/api/v1/topology/:id/activate" [id]
    (with-nimbus nimbus
      (let [id (url-decode id)
            tplg (.getTopologyInfo ^Nimbus$Client nimbus id)
            name (.get_name tplg)]
        (.activate nimbus name)
        (log-message "Activating topology '" name "'")))
    (resp/redirect (str "/api/v1/topology/" id)))

  (POST "/api/v1/topology/:id/deactivate" [id]
    (with-nimbus nimbus
      (let [id (url-decode id)
            tplg (.getTopologyInfo ^Nimbus$Client nimbus id)
            name (.get_name tplg)]
        (.deactivate nimbus name)
        (log-message "Deactivating topology '" name "'")))
    (resp/redirect (str "/api/v1/topology/" id)))
  (POST "/api/v1/topology/:id/rebalance/:wait-time" [id wait-time]
    (with-nimbus nimbus
      (let [id (url-decode id)
            tplg (.getTopologyInfo ^Nimbus$Client nimbus id)
            name (.get_name tplg)
            options (RebalanceOptions.)]
        (.set_wait_secs options (Integer/parseInt wait-time))
        (.rebalance nimbus name options)
        (log-message "Rebalancing topology '" name "' with wait time: " wait-time " secs")))
    (resp/redirect (str "/api/v1/topology/" id)))
  (POST "/api/v1/topology/:id/kill/:wait-time" [id wait-time]
    (with-nimbus nimbus
      (let [id (url-decode id)
            tplg (.getTopologyInfo ^Nimbus$Client nimbus id)
            name (.get_name tplg)
            options (KillOptions.)]
        (.set_wait_secs options (Integer/parseInt wait-time))
        (.killTopologyWithOpts nimbus name options)
        (log-message "Killing topology '" name "' with wait time: " wait-time " secs")))
    (resp/redirect (str "/api/v1/topology/" id)))

  (GET "/" [:as {cookies :cookies}]
       (resp/redirect "/index.html"))
  (route/resources "/")
  (route/not-found "Page not found"))

(defn exception->json
  [ex]
  {"error" "Internal Server Error"
   "errorMessage"
   (let [sw (java.io.StringWriter.)]
     (.printStackTrace ex (java.io.PrintWriter. sw))
     (.toString sw))})

(defn catch-errors
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception ex
        (json-response (exception->json ex) 500)))))

(def app
  (handler/site (-> main-routes
                    (wrap-reload '[backtype.storm.ui.core])
                    catch-errors)))

(defn start-server!
  []
  (run-jetty app {:port (Integer. (*STORM-CONF* UI-PORT))
                  :join? false}))

(defn -main [] (start-server!))
