(ns backtype.storm.ui.core
  (:use compojure.core)
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util])
  (:use [backtype.storm.ui helpers])
  (:use [backtype.storm.daemon [common :only [ACKER-COMPONENT-ID]]])
  (:use [clojure.contrib.seq-utils :only [find-first]])
  (:use [ring.adapter.jetty :only [run-jetty]])
  (:import [backtype.storm.generated TaskSpecificStats
            TaskStats TaskSummary TopologyInfo SpoutStats BoltStats
            ErrorInfo ClusterSummary SupervisorSummary TopologySummary
            Nimbus$Client StormTopology GlobalStreamId])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [backtype.storm [thrift :as thrift]])
  (:gen-class))

(def *STORM-CONF* (read-storm-config))

(defmacro with-nimbus [nimbus-sym & body]
  `(thrift/with-nimbus-connection [~nimbus-sym "localhost" (*STORM-CONF* NIMBUS-THRIFT-PORT)]
     ~@body
     ))

(defn get-filled-stats [summs]
  (->> summs
       (map #(.get_stats ^TaskSummary %))
       (filter not-nil?)))

(defn ui-template [body]
  (html
   [:head
    [:title "Storm UI"]
    (include-css "/css/bootstrap-1.1.0.css")
    (include-js "/js/jquery-1.6.2.min.js")
    (include-js "/js/jquery.tablesorter.min.js")
    ]
   [:script "$.tablesorter.addParser({ 
        id: 'stormtimestr', 
        is: function(s) { 
            return false; 
        }, 
        format: function(s) {
            if(s.search('All time')!=-1) {
              return 1000000000;
            }
            var total = 0;
            $.each(s.split(' '), function(i, v) {
              var amt = parseInt(v);
              if(v.search('ms')!=-1) {
                total += amt;
              } else if (v.search('s')!=-1) {
                total += amt * 1000;
              } else if (v.search('m')!=-1) {
                total += amt * 1000 * 60;
              } else if (v.search('h')!=-1) {
                total += amt * 1000 * 60 * 60;
              } else if (v.search('d')!=-1) {
                total += amt * 1000 * 60 * 60 * 24;
              }
            });
            return total;
        }, 
        type: 'numeric' 
    }); "]
   [:body
    [:h1 (link-to "/" "Storm UI")]
    (seq body)
    ]))

(defn cluster-summary-table [^ClusterSummary summ]
  (let [sups (.get_supervisors summ)
        used-slots (reduce + (map #(.get_num_used_workers ^SupervisorSummary %) sups))
        total-slots (reduce + (map #(.get_num_workers ^SupervisorSummary %) sups))
        free-slots (- total-slots used-slots)
        total-tasks (->> (.get_topologies summ)
                         (map #(.get_num_tasks ^TopologySummary %))
                         (reduce +))]
    (table ["Nimbus uptime" "Supervisors" "Used slots" "Free slots" "Total slots" "Running tasks"]
           [[(pretty-uptime-sec (.get_nimbus_uptime_secs summ))
             (count sups)
             used-slots
             free-slots
             total-slots
             total-tasks]])
    ))

(defn topology-link
  ([id] (topology-link id id))
  ([id content]
     (link-to (format "/topology/%s" id) content)))

(defn main-topology-summary-table [summs]
  ;; make the id clickable
  ;; make the table sortable
  (sorted-table
   ["Name" "Id" "Uptime" "Num workers" "Num tasks"]
   (for [^TopologySummary t summs]
     [(topology-link (.get_id t) (.get_name t))
      (.get_id t)
      (pretty-uptime-sec (.get_uptime_secs t))
      (.get_num_workers t)
      (.get_num_tasks t)
      ])
   :time-cols [2]
   :sort-list "[[2,1]]"
   ))

(defn supervisor-summary-table [summs]
  (sorted-table 
   ["Host" "Uptime" "Slots" "Used slots"]
   (for [^SupervisorSummary s summs]
     [(.get_host s)
      (pretty-uptime-sec (.get_uptime_secs s))
      (.get_num_workers s)
      (.get_num_used_workers s)])
   :time-cols [1]))

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
       ))))

(defn component-type [^StormTopology topology id]
  (let [bolts (.get_bolts topology)
        spouts (.get_spouts topology)]
    (cond
     (.containsKey bolts id) :bolt
     (.containsKey spouts id) :spout
     (= ACKER-COMPONENT-ID id) :bolt
     )))

(defn task-summary-type [topology ^TaskSummary s]
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
  {:emitted (aggregate-counts (map #(.get_emitted ^TaskStats %) stats-seq))
   :transferred (aggregate-counts (map #(.get_transferred ^TaskStats %) stats-seq))}
  )

(defn aggregate-bolt-stats [stats-seq]
  (let [stats-seq (collectify stats-seq)]
    (merge (aggregate-common-stats stats-seq)
           {:acked
            (aggregate-counts (map #(.. ^TaskStats % get_specific get_bolt get_acked)
                                   stats-seq))
            :failed
            (aggregate-counts (map #(.. ^TaskStats % get_specific get_bolt get_failed)
                                   stats-seq))
            :process-latencies
            (aggregate-averages (map #(.. ^TaskStats % get_specific get_bolt get_process_ms_avg)
                                     stats-seq)
                                (map #(.. ^TaskStats % get_specific get_bolt get_acked)
                                     stats-seq))}
           )))

(defn aggregate-spout-stats [stats-seq]
  (let [stats-seq (collectify stats-seq)]
    (merge (aggregate-common-stats stats-seq)
           {:acked
            (aggregate-counts (map #(.. ^TaskStats % get_specific get_spout get_acked)
                                   stats-seq))
            :failed
            (aggregate-counts (map #(.. ^TaskStats % get_specific get_spout get_failed)
                                   stats-seq))
            :complete-latencies
            (aggregate-averages (map #(.. ^TaskStats % get_specific get_spout get_complete_ms_avg)
                                     stats-seq)
                                (map #(.. ^TaskStats % get_specific get_spout get_acked)
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
  (= :spout (task-summary-type topology s)))

(defn bolt-summary? [topology s]
  (= :bolt (task-summary-type topology s)))

(defn topology-summary-table [^TopologyInfo summ]
  (let [tasks (.get_tasks summ)
        workers (set (for [^TaskSummary t tasks] [(.get_host t) (.get_port t)]))]
    (table ["Name" "Id" "Uptime" "Num workers" "Num tasks"]
           [[(.get_name summ)
             (.get_id summ)
             (pretty-uptime-sec (.get_uptime_secs summ))
             (count workers)
             (count tasks)
             ]]
           )))

(defn total-aggregate-stats [spout-summs bolt-summs]
  (let [spout-stats (get-filled-stats spout-summs)
        bolt-stats (get-filled-stats bolt-summs)
        agg-spout-stats (-> spout-stats
                            aggregate-spout-stats
                            aggregate-spout-streams)
        agg-bolt-stats (-> bolt-stats
                           aggregate-bolt-stats
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
                 (format "/topology/%s?window=%s" id k)
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
  (let [ret (group-by #(.get_component_id ^TaskSummary %) summs)]
    (into (sorted-map) ret )))

(defn error-subset [error-str]
  (apply str (take 200 error-str)))

(defn most-recent-error [summs]
  (let [summs (collectify summs)
        error (->> summs
                   (mapcat #(clojurify-structure (.get_errors ^TaskSummary %)))
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
  (link-to (format "/topology/%s/component/%s" storm-id id) id))

(defn spout-comp-table [top-id summ-map window]
  (sorted-table
   ["Id" "Parallelism" "Emitted" "Transferred" "Complete latency (ms)"
    "Acked" "Failed" "Last error"]
   (for [[id summs] summ-map
         :let [stats-seq (get-filled-stats summs)
               stats (aggregate-spout-streams
                      (aggregate-spout-stats
                       stats-seq))]]
     [(component-link top-id id)
      (count summs)
      (get-in stats [:emitted window])
      (get-in stats [:transferred window])
      (float-str (get-in stats [:complete-latencies window]))
      (get-in stats [:acked window])
      (get-in stats [:failed window])
      (most-recent-error summs)
      ]
     )))

(defn bolt-comp-table [top-id summ-map window]
  (sorted-table
   ["Id" "Parallelism" "Emitted" "Transferred" "Process latency (ms)"
    "Acked" "Failed" "Last error"]
   (for [[id summs] summ-map
         :let [stats-seq (get-filled-stats summs)
               stats (aggregate-bolt-streams
                      (aggregate-bolt-stats
                       stats-seq))
               ]]
     [(component-link top-id id)
      (count summs)
      (get-in stats [:emitted window])
      (get-in stats [:transferred window])
      (float-str (get-in stats [:process-latencies window]))
      (get-in stats [:acked window])
      (get-in stats [:failed window])
      (most-recent-error summs)
      ]
     )))

(defn window-hint [window]
  (if (= window ":all-time")
    "All time"
    (pretty-uptime-sec window)))

(defn topology-page [id window]
  (with-nimbus nimbus
    (let [window (if window window ":all-time")
          window-hint (window-hint window)
          summ (.getTopologyInfo ^Nimbus$Client nimbus id)
          topology (.getTopology ^Nimbus$Client nimbus id)
          spout-summs (filter (partial spout-summary? topology) (.get_tasks summ))
          bolt-summs (filter (partial bolt-summary? topology) (.get_tasks summ))
          spout-comp-summs (group-by-comp spout-summs)
          bolt-comp-summs (group-by-comp bolt-summs)
          ]
      (concat
       [[:h2 "Topology summary"]]
       [(topology-summary-table summ)]
       [[:h2 "Topology stats"]]
       (topology-stats-table id window (total-aggregate-stats spout-summs bolt-summs))
       [[:h2 "Spouts (" window-hint ")"]]
       (spout-comp-table id spout-comp-summs window)
       [[:h2 "Bolts (" window-hint ")"]]
       (bolt-comp-table id bolt-comp-summs window)
       ))))

(defn component-task-summs [^TopologyInfo summ topology id]
  (let [spout-summs (filter (partial spout-summary? topology) (.get_tasks summ))
        bolt-summs (filter (partial bolt-summary? topology) (.get_tasks summ))
        spout-comp-summs (group-by-comp spout-summs)
        bolt-comp-summs (group-by-comp bolt-summs)
        ret (if (contains? spout-comp-summs id)
              (spout-comp-summs id)
              (bolt-comp-summs id))]
    (sort-by #(.get_task_id ^TaskSummary %) ret)
    ))

(defn task-link [topology-id id & {:keys [suffix] :or {suffix ""}}]
  (link-to (format "/topology/%s/task/%s%s" topology-id id suffix)
           id))

(defn spout-summary-table [topology-id id stats window]
  (let [times (stats-times (:emitted stats))
        display-map (into {} (for [t times] [t pretty-uptime-sec]))
        display-map (assoc display-map ":all-time" (fn [_] "All time"))]
    (sorted-table
     ["Window" "Emitted" "Transferred" "Complete latency (ms)" "Acked" "Failed"]
     (for [k (concat times [":all-time"])
           :let [disp ((display-map k) k)]]
       [(link-to (if (= k window) {:class "red"} {})
                 (format "/topology/%s/component/%s?window=%s" topology-id id k)
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

(defn spout-task-table [topology-id tasks window]
  (sorted-table
   ["Id" "Uptime" "Host" "Port" "Emitted" "Transferred"
    "Complete latency (ms)" "Acked" "Failed" "Last error"]
   (for [^TaskSummary t tasks
         :let [stats (.get_stats t)
               stats (if stats
                       (-> stats
                           aggregate-spout-stats
                           aggregate-spout-streams
                           swap-map-order
                           (get window)))]]
     [(task-link topology-id (.get_task_id t))
      (pretty-uptime-sec (.get_uptime_secs t))
      (.get_host t)
      (.get_port t)
      (nil-to-zero (:emitted stats))
      (nil-to-zero (:transferred stats))
      (float-str (:complete-latencies stats))
      (nil-to-zero (:acked stats))
      (nil-to-zero (:failed stats))
      (most-recent-error t)
      ]
     )
   :time-cols [1]
   ))

(defn spout-page [window ^TopologyInfo topology-info component tasks]
  (let [window-hint (str " (" (window-hint window) ")")
        stats (get-filled-stats tasks)
        stream-summary (-> stats aggregate-spout-stats)
        summary (-> stream-summary aggregate-spout-streams)]
    (concat
     [[:h2 "Spout stats"]]
     (spout-summary-table (.get_id topology-info) component summary window)
     [[:h2 "Output stats" window-hint]]
     (spout-output-summary-table stream-summary window)
     [[:h2 "Tasks" window-hint]]
     (spout-task-table (.get_id topology-info) tasks window)
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
                           (select-keys [:acked :failed :process-latencies])
                           swap-map-order)]
    (sorted-table
     ["Component" "Stream" "Process latency (ms)" "Acked" "Failed"]
     (for [[^GlobalStreamId s stats] stream-summary]
       [(.get_componentId s)
        (.get_streamId s)
        (float-str (:process-latencies stats))
        (nil-to-zero (:acked stats))
        (nil-to-zero (:failed stats))
        ])
     )))

(defn bolt-task-table [topology-id tasks window]
  (sorted-table
   ["Id" "Uptime" "Host" "Port" "Emitted" "Transferred"
    "Process latency (ms)" "Acked" "Failed" "Last error"]
   (for [^TaskSummary t tasks
         :let [stats (.get_stats t)
               stats (if stats
                       (-> stats
                           aggregate-bolt-stats
                           aggregate-bolt-streams
                           swap-map-order
                           (get window)))]]
     [(task-link topology-id (.get_task_id t))
      (pretty-uptime-sec (.get_uptime_secs t))
      (.get_host t)
      (.get_port t)
      (nil-to-zero (:emitted stats))
      (nil-to-zero (:transferred stats))
      (float-str (:process-latencies stats))
      (nil-to-zero (:acked stats))
      (nil-to-zero (:failed stats))
      (most-recent-error t)
      ]
     )
   :time-cols [1]
   ))

(defn bolt-summary-table [topology-id id stats window]
  (let [times (stats-times (:emitted stats))
        display-map (into {} (for [t times] [t pretty-uptime-sec]))
        display-map (assoc display-map ":all-time" (fn [_] "All time"))]
    (sorted-table
     ["Window" "Emitted" "Transferred" "Process latency (ms)" "Acked" "Failed"]
     (for [k (concat times [":all-time"])
           :let [disp ((display-map k) k)]]
       [(link-to (if (= k window) {:class "red"} {})
                 (format "/topology/%s/component/%s?window=%s" topology-id id k)
                 disp)
        (get-in stats [:emitted k])
        (get-in stats [:transferred k])
        (float-str (get-in stats [:process-latencies k]))
        (get-in stats [:acked k])
        (get-in stats [:failed k])
        ])
     :time-cols [0])))

(defn bolt-page [window ^TopologyInfo topology-info component tasks]
  (let [window-hint (str " (" (window-hint window) ")")
        stats (get-filled-stats tasks)
        stream-summary (-> stats aggregate-bolt-stats)
        summary (-> stream-summary aggregate-bolt-streams)]
    (concat
     [[:h2 "Bolt stats"]]
     (bolt-summary-table (.get_id topology-info) component summary window)

     [[:h2 "Input stats" window-hint]]
     (bolt-input-summary-table stream-summary window)
     
     [[:h2 "Output stats" window-hint]]
     (bolt-output-summary-table stream-summary window)

     [[:h2 "Tasks"]]
     (bolt-task-table (.get_id topology-info) tasks window)
     )))

(defn component-page [topology-id component window]
  (with-nimbus nimbus
    (let [window (if window window ":all-time")
          summ (.getTopologyInfo ^Nimbus$Client nimbus topology-id)
          topology (.getTopology ^Nimbus$Client nimbus topology-id)
          type (component-type topology component)
          summs (component-task-summs summ topology component)
          spec (cond (= type :spout) (spout-page window summ component summs)
                     (= type :bolt) (bolt-page window summ component summs))]
      (concat
       [[:h2 "Component summary"]
        (table ["Id" "Topology" "Parallelism"]
               [[component
                 (topology-link (.get_id summ) (.get_name summ))
                 (count summs)
                 ]])]
       spec
       ))))

(defn errors-table [^TaskSummary task]
  (let [errors (->> task
                    .get_errors
                    (sort-by #(.get_error_time_secs ^ErrorInfo %))
                    reverse)]
    (sorted-table
     ["Time" "Error"]
     (for [^ErrorInfo e errors]
       [(date-str (.get_error_time_secs e))
        [:pre (.get_error e)]])
     :sort-list "[[0,1]]"
     )))

(defn task-summary-table [^TaskSummary task ^TopologyInfo summ]
  (table ["Id" "Topology" "Component" "Uptime" "Host" "Port"]
         [[(.get_task_id task)
            (topology-link (.get_id summ) (.get_name summ))
            (component-link (.get_id summ) (.get_component_id task))
            (pretty-uptime-sec (.get_uptime_secs task))
            (.get_host task)
            (.get_port task)]]
         ))

(defn task-page [topology-id task-id window]
  (with-nimbus nimbus
    (let [summ (.getTopologyInfo ^Nimbus$Client nimbus topology-id)
          topology (.getTopology ^Nimbus$Client nimbus topology-id)
          task (->> summ
                    .get_tasks
                    (find-first #(= (.get_task_id ^TaskSummary %) task-id)))]
      (concat
       [[:h2 "Task summary"]]
       [(task-summary-table task summ)]
       ;; TODO: overall stats -> window, ...
       ;; TODO: inputs/outputs stream stats
       [[:h2 "Errors"]]
       (errors-table task)
       ))))


(defroutes main-routes
  (GET "/" []
       (-> (main-page)
           ui-template))
  (GET "/topology/:id" [id & m]
       (-> (topology-page id (:window m))
           ui-template))
  (GET "/topology/:id/component/:component" [id component & m]
       (-> (component-page id (Integer/parseInt component) (:window m))
           ui-template))
  (GET "/topology/:id/task/:task" [id task & m]
       (-> (task-page id (Integer/parseInt task) (:window m))
           ui-template))
  (route/resources "/")
  (route/not-found "Page not found"))

(def app
  (handler/site main-routes))

(defn -main []
  (run-jetty app {:port 8080}))
