(ns backtype.storm.ui.core
  (:use compojure.core)
  (:use ring.middleware.reload)
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util log])
  (:use [backtype.storm.ui helpers])
  (:use [backtype.storm.daemon [common :only [ACKER-COMPONENT-ID system-id?]]])
  (:use [ring.adapter.jetty :only [run-jetty]])
  (:use [clojure.string :only [trim]])
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

(defn mk-system-toggle-button [include-sys?]
  [:p {:class "js-only"}
    [:input {:type "button"
             :value (str (if include-sys? "Hide" "Show") " System Stats")
             :onclick "toggleSys()"}]])

(defn ui-template [body]
  (html4
   [:head
    [:title "Storm UI"]
    (include-css "/css/bootstrap-1.1.0.css")
    (include-css "/css/style.css")
    (include-js "/js/jquery-1.6.2.min.js")
    (include-js "/js/jquery.tablesorter.min.js")
    (include-js "/js/jquery.cookies.2.2.0.min.js")
    (include-js "/js/script.js")
    ]
   [:body
    [:h1 (link-to "/" "Storm UI")]
    (seq body)
    ]))

(defn read-storm-version []
  (let [storm-home (System/getProperty "storm.home")
        release-path (format "%s/RELEASE" storm-home)
        release-file (File. release-path)]
    (if (and (.exists release-file) (.isFile release-file))
      (trim (slurp release-path))
      "Unknown")))

(defn cluster-summary-table [^ClusterSummary summ]
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
    (table ["Version" "Nimbus uptime" "Supervisors" "Used slots" "Free slots" "Total slots" "Executors" "Tasks"]
           [[(read-storm-version)
             (pretty-uptime-sec (.get_nimbus_uptime_secs summ))
             (count sups)
             used-slots
             free-slots
             total-slots
             total-executors
             total-tasks]])
    ))

(defn topology-link
  ([id] (topology-link id id))
  ([id content]
     (link-to (url-format "/topology/%s" id) (escape-html content))))

(defn main-topology-summary-table [summs]
  ;; make the id clickable
  ;; make the table sortable
  (sorted-table
   ["Name" "Id" "Status" "Uptime" "Num workers" "Num executors" "Num tasks"]
   (for [^TopologySummary t summs]
     [(topology-link (.get_id t) (.get_name t))
      (escape-html (.get_id t))
      (.get_status t)
      (pretty-uptime-sec (.get_uptime_secs t))
      (.get_num_workers t)
      (.get_num_executors t)
      (.get_num_tasks t)
      ])
   :time-cols [3]
   :sort-list "[[0,0]]"
   ))

(defn supervisor-summary-table [summs]
  (sorted-table
   ["Id" "Host" "Uptime" "Slots" "Used slots"]
   (for [^SupervisorSummary s summs]
     [(.get_supervisor_id s)
      (.get_host s)
      (pretty-uptime-sec (.get_uptime_secs s))
      (.get_num_workers s)
      (.get_num_used_workers s)])
   :time-cols [2]))

(defn configuration-table [conf]
  (sorted-table ["Key" "Value"]
    (map #(vector (key %) (str (val %))) conf)))

(defn main-page []
  (with-nimbus nimbus
    (let [summ (.getClusterInfo ^Nimbus$Client nimbus)]
      (concat
       [[:h2 "Cluster Summary"]]
       [(cluster-summary-table summ)]
       [[:h2 "Topology summary"]]
       (main-topology-summary-table (.get_topologies summ))
       [[:h2 "Supervisor summary"]]
       (supervisor-summary-table (.get_supervisors summ))
       [[:h2 "Nimbus Configuration"]]
       (configuration-table (from-json (.getNimbusConf ^Nimbus$Client nimbus)))
       ))))

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

(defn topology-summary-table [^TopologyInfo summ]
  (let [executors (.get_executors summ)
        workers (set (for [^ExecutorSummary e executors] [(.get_host e) (.get_port e)]))]
    (table ["Name" "Id" "Status" "Uptime" "Num workers" "Num executors" "Num tasks"]
           [[(escape-html (.get_name summ))
             (escape-html (.get_id summ))
             (.get_status summ)
             (pretty-uptime-sec (.get_uptime_secs summ))
             (count workers)
             (count executors)
             (sum-tasks executors)
             ]]
           )))

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

(defn topology-stats-table [id window stats]
  (let [times (stats-times (:emitted stats))
        display-map (into {} (for [t times] [t pretty-uptime-sec]))
        display-map (assoc display-map ":all-time" (fn [_] "All time"))]
    (sorted-table
     ["Window" "Emitted" "Transferred" "Complete latency (ms)" "Acked" "Failed"]
     (for [k (concat times [":all-time"])
           :let [disp ((display-map k) k)]]
       [(link-to (if (= k window) {:class "red"} {})
                 (url-format "/topology/%s?window=%s" id k)
                 disp)
        (get-in stats [:emitted k])
        (get-in stats [:transferred k])
        (float-str (get-in stats [:complete-latencies k]))
        (get-in stats [:acked k])
        (get-in stats [:failed k])
        ]
       )
     :time-cols [0]
     )))

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

(defn component-link [storm-id id]
  (link-to (url-format "/topology/%s/component/%s" storm-id id) (escape-html id)))

(defn worker-log-link [host port]
  (link-to (url-format "http://%s:%s/log?file=worker-%s.log"
              host (*STORM-CONF* LOGVIEWER-PORT) port) (str port)))

(defn render-capacity [capacity]
  (let [capacity (nil-to-zero capacity)]
    [:span (if (> capacity 0.9)
                 {:class "red"}
                 {})
           (float-str capacity)]))

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

(defn spout-comp-table [top-id summ-map errors window include-sys?]
  (sorted-table
   ["Id" "Executors" "Tasks" "Emitted" "Transferred" "Complete latency (ms)"
    "Acked" "Failed" "Last error"]
   (for [[id summs] summ-map
         :let [stats-seq (get-filled-stats summs)
               stats (aggregate-spout-streams
                      (aggregate-spout-stats
                       stats-seq include-sys?))]]
     [(component-link top-id id)
      (count summs)
      (sum-tasks summs)
      (get-in stats [:emitted window])
      (get-in stats [:transferred window])
      (float-str (get-in stats [:complete-latencies window]))
      (get-in stats [:acked window])
      (get-in stats [:failed window])
      (most-recent-error (get errors id))
      ]
     )))

(defn bolt-comp-table [top-id summ-map errors window include-sys?]
  (sorted-table
   ["Id" "Executors" "Tasks" "Emitted" "Transferred" "Capacity (last 10m)" "Execute latency (ms)" "Executed" "Process latency (ms)"
    "Acked" "Failed" "Last error"]
   (for [[id summs] summ-map
         :let [stats-seq (get-filled-stats summs)
               stats (aggregate-bolt-streams
                      (aggregate-bolt-stats
                       stats-seq include-sys?))
               ]]
     [(component-link top-id id)
      (count summs)
      (sum-tasks summs)
      (get-in stats [:emitted window])
      (get-in stats [:transferred window])
      (render-capacity (compute-bolt-capacity summs))
      (float-str (get-in stats [:execute-latencies window]))
      (get-in stats [:executed window])
      (float-str (get-in stats [:process-latencies window]))
      (get-in stats [:acked window])
      (get-in stats [:failed window])
      (most-recent-error (get errors id))
      ]
     )))

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
      (concat
       [[:h2 "Topology summary"]]
       [(topology-summary-table summ)]
       [[:h2 {:class "js-only"} "Topology actions"]]
       [[:p {:class "js-only"} (concat
         [(topology-action-button id name "Activate" "activate" false 0 (= "INACTIVE" status))]
         [(topology-action-button id name "Deactivate" "deactivate" false 0 (= "ACTIVE" status))]
         [(topology-action-button id name "Rebalance" "rebalance" true msg-timeout (or (= "ACTIVE" status) (= "INACTIVE" status)))]
         [(topology-action-button id name "Kill" "kill" true msg-timeout (not= "KILLED" status))]
       )]]
       [[:h2 "Topology stats"]]
       (topology-stats-table id window (total-aggregate-stats spout-summs bolt-summs include-sys?))
       [[:h2 "Spouts (" window-hint ")"]]
       (spout-comp-table id spout-comp-summs (.get_errors summ) window include-sys?)
       [[:h2 "Bolts (" window-hint ")"]]
       (bolt-comp-table id bolt-comp-summs (.get_errors summ) window include-sys?)
       [[:h2 "Topology Configuration"]]
       (configuration-table topology-conf)
       ))))

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

(defn spout-summary-table [topology-id id stats window]
  (let [times (stats-times (:emitted stats))
        display-map (into {} (for [t times] [t pretty-uptime-sec]))
        display-map (assoc display-map ":all-time" (fn [_] "All time"))]
    (sorted-table
     ["Window" "Emitted" "Transferred" "Complete latency (ms)" "Acked" "Failed"]
     (for [k (concat times [":all-time"])
           :let [disp ((display-map k) k)]]
       [(link-to (if (= k window) {:class "red"} {})
                 (url-format "/topology/%s/component/%s?window=%s" topology-id id k)
                 disp)
        (get-in stats [:emitted k])
        (get-in stats [:transferred k])
        (float-str (get-in stats [:complete-latencies k]))
        (get-in stats [:acked k])
        (get-in stats [:failed k])
        ])
     :time-cols [0])))

(defn spout-output-summary-table [stream-summary window]
  (let [stream-summary (map-val swap-map-order (swap-map-order stream-summary))]
    (sorted-table
     ["Stream" "Emitted" "Transferred" "Complete latency (ms)" "Acked" "Failed"]
     (for [[s stats] (stream-summary window)]
       [s
        (nil-to-zero (:emitted stats))
        (nil-to-zero (:transferred stats))
        (float-str (:complete-latencies stats))
        (nil-to-zero (:acked stats))
        (nil-to-zero (:failed stats))])
     )))

(defn spout-executor-table [topology-id executors window include-sys?]
  (sorted-table
   ["Id" "Uptime" "Host" "Port" "Emitted" "Transferred"
    "Complete latency (ms)" "Acked" "Failed"]
   (for [^ExecutorSummary e executors
         :let [stats (.get_stats e)
               stats (if stats
                       (-> stats
                           (aggregate-spout-stats include-sys?)
                           aggregate-spout-streams
                           swap-map-order
                           (get window)))]]
     [(pretty-executor-info (.get_executor_info e))
      (pretty-uptime-sec (.get_uptime_secs e))
      (.get_host e)
      (worker-log-link (.get_host e) (.get_port e))
      (nil-to-zero (:emitted stats))
      (nil-to-zero (:transferred stats))
      (float-str (:complete-latencies stats))
      (nil-to-zero (:acked stats))
      (nil-to-zero (:failed stats))
      ]
     )
   :time-cols [1]
   ))

(defn spout-page [window ^TopologyInfo topology-info component executors include-sys?]
  (let [window-hint (str " (" (window-hint window) ")")
        stats (get-filled-stats executors)
        stream-summary (-> stats (aggregate-spout-stats include-sys?))
        summary (-> stream-summary aggregate-spout-streams)]
    (concat
     [[:h2 "Spout stats"]]
     (spout-summary-table (.get_id topology-info) component summary window)
     [[:h2 "Output stats" window-hint]]
     (spout-output-summary-table stream-summary window)
     [[:h2 "Executors" window-hint]]
     (spout-executor-table (.get_id topology-info) executors window include-sys?)
     ;; task id, task uptime, stream aggregated stats, last error
     )))

(defn bolt-output-summary-table [stream-summary window]
  (let [stream-summary (-> stream-summary
                           swap-map-order
                           (get window)
                           (select-keys [:emitted :transferred])
                           swap-map-order)]
    (sorted-table
     ["Stream" "Emitted" "Transferred"]
     (for [[s stats] stream-summary]
       [s
        (nil-to-zero (:emitted stats))
        (nil-to-zero (:transferred stats))
        ])
     )))

(defn bolt-input-summary-table [stream-summary window]
  (let [stream-summary (-> stream-summary
                           swap-map-order
                           (get window)
                           (select-keys [:acked :failed :process-latencies :executed :execute-latencies])
                           swap-map-order)]
    (sorted-table
     ["Component" "Stream" "Execute latency (ms)" "Executed" "Process latency (ms)" "Acked" "Failed"]
     (for [[^GlobalStreamId s stats] stream-summary]
       [(escape-html (.get_componentId s))
        (.get_streamId s)
        (float-str (:execute-latencies stats))
        (nil-to-zero (:executed stats))
        (float-str (:process-latencies stats))
        (nil-to-zero (:acked stats))
        (nil-to-zero (:failed stats))
        ])
     )))

(defn bolt-executor-table [topology-id executors window include-sys?]
  (sorted-table
   ["Id" "Uptime" "Host" "Port" "Emitted" "Transferred" "Capacity (last 10m)"
    "Execute latency (ms)" "Executed" "Process latency (ms)" "Acked" "Failed"]
   (for [^ExecutorSummary e executors
         :let [stats (.get_stats e)
               stats (if stats
                       (-> stats
                           (aggregate-bolt-stats include-sys?)
                           (aggregate-bolt-streams)
                           swap-map-order
                           (get window)))]]
     [(pretty-executor-info (.get_executor_info e))
      (pretty-uptime-sec (.get_uptime_secs e))
      (.get_host e)
      (worker-log-link (.get_host e) (.get_port e))
      (nil-to-zero (:emitted stats))
      (nil-to-zero (:transferred stats))
      (render-capacity (compute-executor-capacity e))
      (float-str (:execute-latencies stats))
      (nil-to-zero (:executed stats))
      (float-str (:process-latencies stats))
      (nil-to-zero (:acked stats))
      (nil-to-zero (:failed stats))
      ]
     )
   :time-cols [1]
   ))

(defn bolt-summary-table [topology-id id stats window]
  (let [times (stats-times (:emitted stats))
        display-map (into {} (for [t times] [t pretty-uptime-sec]))
        display-map (assoc display-map ":all-time" (fn [_] "All time"))]
    (sorted-table
     ["Window" "Emitted" "Transferred" "Execute latency (ms)" "Executed" "Process latency (ms)" "Acked" "Failed"]
     (for [k (concat times [":all-time"])
           :let [disp ((display-map k) k)]]
       [(link-to (if (= k window) {:class "red"} {})
                 (url-format "/topology/%s/component/%s?window=%s" topology-id id k)
                 disp)
        (get-in stats [:emitted k])
        (get-in stats [:transferred k])
        (float-str (get-in stats [:execute-latencies k]))
        (get-in stats [:executed k])
        (float-str (get-in stats [:process-latencies k]))
        (get-in stats [:acked k])
        (get-in stats [:failed k])
        ])
     :time-cols [0])))

(defn bolt-page [window ^TopologyInfo topology-info component executors include-sys?]
  (let [window-hint (str " (" (window-hint window) ")")
        stats (get-filled-stats executors)
        stream-summary (-> stats (aggregate-bolt-stats include-sys?))
        summary (-> stream-summary aggregate-bolt-streams)]
    (concat
     [[:h2 "Bolt stats"]]
     (bolt-summary-table (.get_id topology-info) component summary window)

     [[:h2 "Input stats" window-hint]]
     (bolt-input-summary-table stream-summary window)

     [[:h2 "Output stats" window-hint]]
     (bolt-output-summary-table stream-summary window)

     [[:h2 "Executors"]]
     (bolt-executor-table (.get_id topology-info) executors window include-sys?)
     )))

(defn errors-table [errors-list]
  (let [errors (->> errors-list
                    (sort-by #(.get_error_time_secs ^ErrorInfo %))
                    reverse)]
    (sorted-table
     ["Time" "Error"]
     (for [^ErrorInfo e errors]
       [(date-str (.get_error_time_secs e))
        [:pre (.get_error e)]])
     :sort-list "[[0,1]]"
     )))

(defn component-page [topology-id component window include-sys?]
  (with-nimbus nimbus
    (let [window (if window window ":all-time")
          summ (.getTopologyInfo ^Nimbus$Client nimbus topology-id)
          topology (.getTopology ^Nimbus$Client nimbus topology-id)
          type (component-type topology component)
          summs (component-task-summs summ topology component)
          spec (cond (= type :spout) (spout-page window summ component summs include-sys?)
                     (= type :bolt) (bolt-page window summ component summs include-sys?))]
      (concat
       [[:h2 "Component summary"]
        (table ["Id" "Topology" "Executors" "Tasks"]
               [[(escape-html component)
                 (topology-link (.get_id summ) (.get_name summ))
                 (count summs)
                 (sum-tasks summs)
                 ]])]
       spec
       [[:h2 "Errors"]
        (errors-table (get (.get_errors summ) component))]
       ))))

(defn get-include-sys? [cookies]
  (let [sys? (get cookies "sys")
        sys? (if (or (nil? sys?) (= "false" (:value sys?))) false true)]
    sys?))

(defroutes main-routes
  (GET "/" [:as {cookies :cookies}]
       (-> (main-page)
           ui-template))
  (GET "/topology/:id" [:as {cookies :cookies} id & m]
       (let [include-sys? (get-include-sys? cookies)]
         (-> (topology-page id (:window m) include-sys?)
             (concat [(mk-system-toggle-button include-sys?)])
             ui-template)))
  (GET "/topology/:id/component/:component" [:as {cookies :cookies} id component & m]
       (let [include-sys? (get-include-sys? cookies)]
         (-> (component-page id component (:window m) include-sys?)
             (concat [(mk-system-toggle-button include-sys?)])
             ui-template)))
  (POST "/topology/:id/activate" [id]
    (with-nimbus nimbus
      (let [tplg (.getTopologyInfo ^Nimbus$Client nimbus id)
            name (.get_name tplg)]
        (.activate nimbus name)
        (log-message "Activating topology '" name "'")))
    (resp/redirect (str "/topology/" id)))
  (POST "/topology/:id/deactivate" [id]
    (with-nimbus nimbus
      (let [tplg (.getTopologyInfo ^Nimbus$Client nimbus id)
            name (.get_name tplg)]
        (.deactivate nimbus name)
        (log-message "Deactivating topology '" name "'")))
    (resp/redirect (str "/topology/" id)))
  (POST "/topology/:id/rebalance/:wait-time" [id wait-time]
    (with-nimbus nimbus
      (let [tplg (.getTopologyInfo ^Nimbus$Client nimbus id)
            name (.get_name tplg)
            options (RebalanceOptions.)]
        (.set_wait_secs options (Integer/parseInt wait-time))
        (.rebalance nimbus name options)
        (log-message "Rebalancing topology '" name "' with wait time: " wait-time " secs")))
    (resp/redirect (str "/topology/" id)))
  (POST "/topology/:id/kill/:wait-time" [id wait-time]
    (with-nimbus nimbus
      (let [tplg (.getTopologyInfo ^Nimbus$Client nimbus id)
            name (.get_name tplg)
            options (KillOptions.)]
        (.set_wait_secs options (Integer/parseInt wait-time))
        (.killTopologyWithOpts nimbus name options)
        (log-message "Killing topology '" name "' with wait time: " wait-time " secs")))
    (resp/redirect (str "/topology/" id)))
  (route/resources "/")
  (route/not-found "Page not found"))

(defn exception->html [ex]
  (concat
    [[:h2 "Internal Server Error"]]
    [[:pre (let [sw (java.io.StringWriter.)]
      (.printStackTrace ex (java.io.PrintWriter. sw))
      (.toString sw))]]))

(defn catch-errors [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception ex
        (-> (resp/response (ui-template (exception->html ex)))
          (resp/status 500)
          (resp/content-type "text/html"))
        ))))

(def app
  (-> #'main-routes
      (wrap-reload '[backtype.storm.ui.core])
      catch-errors))

(defn start-server! [] (run-jetty app {:port (Integer. (*STORM-CONF* UI-PORT))
                                       :join? false}))

(defn -main [] (start-server!))
