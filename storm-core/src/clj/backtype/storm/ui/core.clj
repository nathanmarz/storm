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
  (:use [backtype.storm.daemon [common :only [ACKER-COMPONENT-ID system-id?]]])
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

(defmacro with-nimbus [nimbus-sym & body]
  `(thrift/with-nimbus-connection [~nimbus-sym (*STORM-CONF* NIMBUS-HOST) (*STORM-CONF* NIMBUS-THRIFT-PORT)]
     ~@body
     ))

(defn get-filled-stats [summs]
  (->> summs
       (map #(.get_stats ^ExecutorSummary %))
       (filter not-nil?)))

(defn read-storm-version []
  (let [storm-home (System/getProperty "storm.home")
        release-path (format "%s/RELEASE" storm-home)
        release-file (File. release-path)]
    (if (and (.exists release-file) (.isFile release-file))
      (trim (slurp release-path))
      "Unknown")))

(defn component-type [^StormTopology topology id]
  (let [bolts (.get_bolts topology)
        spouts (.get_spouts topology)]
    (cond
     (.containsKey bolts id) :bolt
     (.containsKey spouts id) :spout
     )))

(defn executor-summary-type [topology ^ExecutorSummary s]
  (component-type topology (.get_component_id s)))

(defn add-pairs
  ([] [0 0])
  ([[a1 a2] [b1 b2]]
      [(+ a1 b1) (+ a2 b2)]))

(defn expand-averages [avg counts]
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
                     ))]
            ))))

(defn expand-averages-seq [average-seq counts-seq]
  (->> (map vector average-seq counts-seq)
       (map #(apply expand-averages %))
       (apply merge-with
              (fn [s1 s2]
                (merge-with
                 add-pairs
                 s1
                 s2)))
       ))

(defn- val-avg [[t c]]
  (if (= t 0) 0
      (double (/ t c))))

(defn aggregate-averages [average-seq counts-seq]
  (->> (expand-averages-seq average-seq counts-seq)
       (map-val
        (fn [s]
          (map-val val-avg s)
          ))
       ))

(defn aggregate-counts [counts-seq]
  (->> counts-seq
       (map clojurify-structure)
       (apply merge-with
              (fn [s1 s2]
                (merge-with + s1 s2))
              )))

(defn aggregate-avg-streams [avg counts]
  (let [expanded (expand-averages avg counts)]
    (->> expanded
         (map-val #(reduce add-pairs (vals %)))
         (map-val val-avg)
         )))

(defn aggregate-count-streams [stats]
  (->> stats
       (map-val #(reduce + (vals %)))))

(defn aggregate-common-stats [stats-seq]
  {:emitted (aggregate-counts (map #(.get_emitted ^ExecutorStats %) stats-seq))
   :transferred (aggregate-counts (map #(.get_transferred ^ExecutorStats %) stats-seq))}
  )

(defn mk-include-sys-fn [include-sys?]
  (if include-sys?
    (fn [_] true)
    (fn [stream] (and (string? stream) (not (system-id? stream))))))

(defn pre-process [stream-summary include-sys?]
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

(defn aggregate-bolt-stats [stats-seq include-sys?]
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
                                     stats-seq))
            })))

(defn aggregate-spout-stats [stats-seq include-sys?]
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
                                     stats-seq))
            }
           )))

(defn aggregate-bolt-streams [stats]
  {:acked (aggregate-count-streams (:acked stats))
   :failed (aggregate-count-streams (:failed stats))
   :emitted (aggregate-count-streams (:emitted stats))
   :transferred (aggregate-count-streams (:transferred stats))
   :process-latencies (aggregate-avg-streams (:process-latencies stats)
                                             (:acked stats))
   :executed (aggregate-count-streams (:executed stats))
   :execute-latencies (aggregate-avg-streams (:execute-latencies stats)
                                             (:executed stats))
   })

(defn aggregate-spout-streams [stats]
  {:acked (aggregate-count-streams (:acked stats))
   :failed (aggregate-count-streams (:failed stats))
   :emitted (aggregate-count-streams (:emitted stats))
   :transferred (aggregate-count-streams (:transferred stats))
   :complete-latencies (aggregate-avg-streams (:complete-latencies stats)
                                              (:acked stats))
   })

(defn spout-summary? [topology s]
  (= :spout (executor-summary-type topology s)))

(defn bolt-summary? [topology s]
  (= :bolt (executor-summary-type topology s)))

(defn group-by-comp [summs]
  (let [ret (group-by #(.get_component_id ^ExecutorSummary %) summs)]
    (into (sorted-map) ret )))

(defn error-subset [error-str]
  (apply str (take 200 error-str)))

(defn most-recent-error [errors-list]
  (let [error (->> errors-list
                   (sort-by #(.get_error_time_secs ^ErrorInfo %))
                   reverse
                   first)]
    (if error
      [:span (if (< (time-delta (.get_error_time_secs ^ErrorInfo error))
                    (* 60 30))
               {:class "red"}
               {})
       (error-subset (.get_error ^ErrorInfo error))]
      )))

(defn component-task-summs [^TopologyInfo summ topology id]
  (let [spout-summs (filter (partial spout-summary? topology) (.get_executors summ))
        bolt-summs (filter (partial bolt-summary? topology) (.get_executors summ))
        spout-comp-summs (group-by-comp spout-summs)
        bolt-comp-summs (group-by-comp bolt-summs)
        ret (if (contains? spout-comp-summs id)
              (spout-comp-summs id)
              (bolt-comp-summs id))]
    (sort-by #(-> ^ExecutorSummary % .get_executor_info .get_task_start) ret)
    ))

(defn worker-log-link [host port]
  (url-format "http://%s:%s/log?file=worker-%s.log"
              host (*STORM-CONF* LOGVIEWER-PORT) port))

(defn compute-executor-capacity [^ExecutorSummary e]
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
        latency (-> stats :execute-latencies nil-to-zero)
        ]
   (if (> window 0)
     (div (* executed latency) (* 1000 window))
     )))

(defn compute-bolt-capacity [executors]
  (->> executors
       (map compute-executor-capacity)
       (map nil-to-zero)
       (apply max)))

(defn total-aggregate-stats [spout-summs bolt-summs include-sys?]
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
     (select-keys agg-bolt-stats [:emitted :transferred])
     agg-spout-stats
     )))

(defn stats-times [stats-map]
  (sort-by #(Integer/parseInt %)
           (-> stats-map
               clojurify-structure
               (dissoc ":all-time")
               keys)))

(defn window-hint [window]
  (if (= window ":all-time")
    "All time"
    (pretty-uptime-sec window)))

(defn topology-action-button [id name action command is-wait default-wait enabled]
  [:input {:type "button"
           :value action
           (if enabled :enabled :disabled) ""
           :onclick (str "confirmAction('"
                         (StringEscapeUtils/escapeJavaScript id) "', '"
                         (StringEscapeUtils/escapeJavaScript name) "', '"
                         command "', " is-wait ", " default-wait ")")}])

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
       { "stormVersion" (read-storm-version)
         "nimbusUptime" (pretty-uptime-sec (.get_nimbus_uptime_secs summ))
         "supervisors" (count sups)
         "topologies" ""
         "slotsTotal" total-slots
         "slotsUsed"  used-slots
         "slotsFree" free-slots
         "executorsTotal" total-executors
         "tasksTotal" total-tasks })))

(defn supervisor-summary
  ([]
     (with-nimbus nimbus
       (supervisor-summary (.get_supervisors (.getClusterInfo ^Nimbus$Client nimbus)))
       ))
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
     (with-nimbus nimbus
       (all-topologies-summary (.get_topologies (.getClusterInfo ^Nimbus$Client nimbus)))))
  ([summs]
     {"topologies"
      (for [^TopologySummary t summs]
        {"id" (.get_id t)
         "name" (.get_name t)
         "status" (.get_status t)
         "uptime" (.get_uptime_secs t)
         "tasksTotal" (.get_num_tasks t)
         "workersTotal" (.get_num_workers t)
         "executorsTotal" (.get_num_executors t)})
      }))

(defn topology-stats [id window stats]
  (let [times (stats-times (:emitted stats))
        display-map (into {} (for [t times] [t pretty-uptime-sec]))
        display-map (assoc display-map ":all-time" (fn [_] "All time"))]
    (for [k (concat times [":all-time"])
           :let [disp ((display-map k) k)]]
      { "windowPretty" disp
        "window" k
        "emitted" (get-in stats [:emitted k])
        "transferred" (get-in stats [:transferred k])
        "completeLatency" (float-str (get-in stats [:complete-latencies k]))
        "acked" (get-in stats [:acked k])
        "failed" (get-in stats [:failed k])
        }
      )))

(defn spout-comp [top-id summ-map errors window include-sys?]
  (for [[id summs] summ-map
        :let [stats-seq (get-filled-stats summs)
              stats (aggregate-spout-streams
                     (aggregate-spout-stats
                      stats-seq include-sys?))]]
    {"spoutId" id
     "executors" (count summs)
     "tasks" (sum-tasks summs)
     "emitted" (get-in stats [:emitted window])
     "transferred" (get-in stats [:transferred window])
     "complete_latency" (float-str (get-in stats [:complete-latencies window]))
     "acked" (get-in stats [:acked window])
     "failed" (get-in stats [:failed window])
     "last_error" (most-recent-error (get errors id))
     }))

(defn bolt-comp [top-id summ-map errors window include-sys?]
  (for [[id summs] summ-map
        :let [stats-seq (get-filled-stats summs)
              stats (aggregate-bolt-streams
                     (aggregate-bolt-stats
                      stats-seq include-sys?))
              ]]
    {"boltId" id
     "executors" (count summs)
     "tasks" (sum-tasks summs)
     "emitted" (get-in stats [:emitted window])
     "trasnferred" (get-in stats [:transferred window])
     "capacity" (compute-bolt-capacity summs)
     "execute_latency" (float-str (get-in stats [:execute-latencies window]))
     "executed" (get-in stats [:executed window])
     "process_latency" (float-str (get-in stats [:process-latencies window]))
     "acked" (get-in stats [:acked window])
     "failed" (get-in stats [:failed window])
     "last_error" (most-recent-error (get errors id))
     }
    ))

(defn topology-summary [^TopologyInfo summ]
  (let [executors (.get_executors summ)
        workers (set (for [^ExecutorSummary e executors] [(.get_host e) (.get_port e)]))]
      {"id" (.get_id summ)
       "name" (.get_name summ)
       "status" (.get_status summ)
       "uptime" (pretty-uptime-sec (.get_uptime_secs summ))
       "tasksTotal" (sum-tasks executors)
       "workersTotal" (count workers)
       "executorsTotal" (count executors)}
      ))

(defn spout-summary [topology-id id stats window]
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
        "failed" (get-in stats [:failed k])
        }
       )))

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
          ]
      (merge
       (topology-summary summ)
       {"window" window
        "windowHint" window-hint
        "msgTimeout" msg-timeout
        "topologyStats" (topology-stats id window (total-aggregate-stats spout-summs bolt-summs include-sys?))
        "spouts" (spout-comp id spout-comp-summs (.get_errors summ) window include-sys?)
        "bolts" (bolt-comp id bolt-comp-summs (.get_errors summ) window include-sys?)
        "configuration" topology-conf})
      )))

(defn spout-output-stats [stream-summary window]
  (let [stream-summary (map-val swap-map-order (swap-map-order stream-summary))]
    (for [[s stats] (stream-summary window)]
      {"stream" s
       "emitted" (nil-to-zero (:emitted stats))
       "transferred" (nil-to-zero (:transferred stats))
       "complete_latency" (float-str (:complete-latencies stats))
       "acked" (nil-to-zero (:acked stats))
       "failed" (nil-to-zero (:failed stats))
       }
      )))

(defn spout-executor-stats [topology-id executors window include-sys?]
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
     "workerLogLink" (worker-log-link (.get_host e) (.get_port e))
     }
  ))

(defn component-errors [errors-list]
  (let [errors (->> errors-list
                    (sort-by #(.get_error_time_secs ^ErrorInfo %))
                    reverse)]
    {"errors"
     (for [^ErrorInfo e errors]
       [{"time" (date-str (.get_error_time_secs e))
         "error" (.get_error e)
         }])}
     ))

(defn spout-stats [window ^TopologyInfo topology-info component executors include-sys?]
  (let [window-hint (str " (" (window-hint window) ")")
        stats (get-filled-stats executors)
        stream-summary (-> stats (aggregate-spout-stats include-sys?))
        summary (-> stream-summary aggregate-spout-streams)]
    {"spoutSummary" (spout-summary (.get_id topology-info) component summary window)
     "outputStats" (spout-output-stats stream-summary window)
     "executorStats" (spout-executor-stats (.get_id topology-info) executors window include-sys?)}
     ))

(defn bolt-summary [topology-id id stats window]
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

(defn bolt-output-stats [stream-summary window]
  (let [stream-summary (-> stream-summary
                           swap-map-order
                           (get window)
                           (select-keys [:emitted :transferred])
                           swap-map-order)]
    (for [[s stats] stream-summary]
      {"stream" s
        "emitted" (nil-to-zero (:emitted stats))
        "transferred" (nil-to-zero (:transferred stats))}
    )))

(defn bolt-input-stats [stream-summary window]
  (let [stream-summary (-> stream-summary
                           swap-map-order
                           (get window)
                           (select-keys [:acked :failed :process-latencies :executed :execute-latencies])
                           swap-map-order)]
    (for [[^GlobalStreamId s stats] stream-summary]
      {"component" (.get_componentId s)
        "stream" (.get_streamId s)
        "executeLatency" (float-str (:execute-latencies stats))
        "processLatency" (float-str (:execute-latencies stats))
        "executed" (nil-to-zero (:executed stats))
        "acked" (nil-to-zero (:acked stats))
        "failed" (nil-to-zero (:failed stats))
        })))

(defn bolt-executor-stats [topology-id executors window include-sys?]
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
      "capacity" (compute-executor-capacity e)
      "executeLatency" (float-str (:execute-latencies stats))
      "executed" (nil-to-zero (:executed stats))
      "processLatency" (float-str (:process-latencies stats))
      "acked" (nil-to-zero (:acked stats))
      "failed" (nil-to-zero (:failed stats))
      "workerLogLink" (worker-log-link (.get_host e) (.get_port e))
      }))

(defn bolt-stats [window ^TopologyInfo topology-info component executors include-sys?]
  (let [window-hint (str " (" (window-hint window) ")")
        stats (get-filled-stats executors)
        stream-summary (-> stats (aggregate-bolt-stats include-sys?))
        summary (-> stream-summary aggregate-bolt-streams)]
    {"boltStats" (bolt-summary (.get_id topology-info) component summary window)
     "inputStats" (bolt-input-stats stream-summary window)
     "outputStats" (bolt-output-stats stream-summary window)
     "executorStats" (bolt-executor-stats (.get_id topology-info) executors window include-sys?)}
    ))

(defn component-page [topology-id component window include-sys?]
  (with-nimbus nimbus
    (let [window (if window window ":all-time")
          summ (.getTopologyInfo ^Nimbus$Client nimbus topology-id)
          topology (.getTopology ^Nimbus$Client nimbus topology-id)
          type (component-type topology component)
          summs (component-task-summs summ topology component)
          spec (cond (= type :spout) (spout-stats window summ component summs include-sys?)
                     (= type :bolt) (bolt-stats window summ component summs include-sys?))
          errors (component-errors (get (.get_errors summ) component))]
      (merge
       {"id" component
         "name" (.get_name summ)
         "executors" (count summs)
         "tasks" (sum-tasks summs)
         "topologyId" topology-id
         "window" window
         "componentType" (name type)
         "windowHint" (window-hint window)
         } spec errors))))

(defn check-include-sys? [sys?]
  (if (or (nil? sys?) (= "false" sys?)) false true))

(defn json-response [data & [status]]
  {:status (or status 200)
   :headers {"Content-Type" "application/json"}
   :body (to-json data)
   })

(defroutes main-routes
  (GET "/api/cluster/configuration" []
       (cluster-configuration))
  (GET "/api/cluster/summary" []
       (json-response (cluster-summary)))
  (GET "/api/supervisor/summary" []
       (json-response (supervisor-summary)))
  (GET "/api/topology/summary" []
       (json-response (all-topologies-summary)))
  (GET  "/api/topology/:id" [id & m]
        (let [id (java.net.URLDecoder/decode id)]
          (json-response (topology-page id (:window m) (check-include-sys? (:sys m))))))
  (GET "/api/topology/:id/component/:component" [id component & m]
       (let [id (java.net.URLDecoder/decode id)
             component (java.net.URLDecoder/decode component)]
         (json-response (component-page id component (:window m) (check-include-sys? (:sys m))))))
  (POST "/api/topology/:id/activate" [id]
    (with-nimbus nimbus
      (let [id (java.net.URLDecoder/decode id)
            tplg (.getTopologyInfo ^Nimbus$Client nimbus id)
            name (.get_name tplg)]
        (.activate nimbus name)
        (log-message "Activating topology '" name "'")))
    (resp/redirect (str "/api/topology/" id)))

  (POST "/api/topology/:id/deactivate" [id]
    (with-nimbus nimbus
      (let [id (java.net.URLDecoder/decode id)
            tplg (.getTopologyInfo ^Nimbus$Client nimbus id)
            name (.get_name tplg)]
        (.deactivate nimbus name)
        (log-message "Deactivating topology '" name "'")))
    (resp/redirect (str "/api/topology/" id)))
  (POST "/api/topology/:id/rebalance/:wait-time" [id wait-time]
    (with-nimbus nimbus
      (let [id (java.net.URLDecoder/decode id)
            tplg (.getTopologyInfo ^Nimbus$Client nimbus id)
            name (.get_name tplg)
            options (RebalanceOptions.)]
        (.set_wait_secs options (Integer/parseInt wait-time))
        (.rebalance nimbus name options)
        (log-message "Rebalancing topology '" name "' with wait time: " wait-time " secs")))
    (resp/redirect (str "/api/topology/" id)))
  (POST "/api/topology/:id/kill/:wait-time" [id wait-time]
    (with-nimbus nimbus
      (let [id (java.net.URLDecoder/decode id)
            tplg (.getTopologyInfo ^Nimbus$Client nimbus id)
            name (.get_name tplg)
            options (KillOptions.)]
        (.set_wait_secs options (Integer/parseInt wait-time))
        (.killTopologyWithOpts nimbus name options)
        (log-message "Killing topology '" name "' with wait time: " wait-time " secs")))
    (resp/redirect (str "/api/topology/" id)))

  (GET "/" [:as {cookies :cookies}]
       (resp/redirect "/index.html"))
  (route/resources "/")
  (route/not-found "Page not found"))

(defn exception->json [ex]
  { "error" "Internal Server Error"
     "errorMessage" (let [sw (java.io.StringWriter.)]
      (.printStackTrace ex (java.io.PrintWriter. sw))
      (.toString sw))
    })

(defn catch-errors [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception ex
        (json-response (exception->json ex) 500)
        ))))

(def app
  (handler/site (-> main-routes
                    (wrap-reload '[backtype.storm.ui.core])
                    catch-errors)))

(defn start-server! [] (run-jetty app {:port (Integer. (*STORM-CONF* UI-PORT))
                                       :join? false}))

(defn -main [] (start-server!))
