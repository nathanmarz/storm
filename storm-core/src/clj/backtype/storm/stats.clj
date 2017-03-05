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
            ClusterSummary TopologyInfo TopologySummary ExecutorSummary ExecutorStats ExecutorSpecificStats
            SpoutStats BoltStats ErrorInfo SupervisorSummary])
  (:use [backtype.storm util])
  (:use [clojure.math.numeric-tower :only [ceil]]))

;;TODO: consider replacing this with some sort of RRD

(defn curr-time-bucket [^Integer time-secs ^Integer bucket-size-secs]
  (* bucket-size-secs (unchecked-divide-int time-secs bucket-size-secs))
  )

(defrecord RollingWindow [updater merger extractor bucket-size-secs num-buckets buckets])

(defn rolling-window [updater merger extractor bucket-size-secs num-buckets]
  (RollingWindow. updater merger extractor bucket-size-secs num-buckets {}))

(defn update-rolling-window
  ([^RollingWindow rw time-secs & args]
     ;; this is 2.5x faster than using update-in...
     (let [time-bucket (curr-time-bucket time-secs (:bucket-size-secs rw))
           buckets (:buckets rw)
           curr (get buckets time-bucket)           
           curr (apply (:updater rw) curr args)
           ]
       (assoc rw :buckets (assoc buckets time-bucket curr))
       )))

(defn value-rolling-window [^RollingWindow rw]
  ((:extractor rw)
   (let [values (vals (:buckets rw))]
     (apply (:merger rw) values)
     )))

(defn cleanup-rolling-window [^RollingWindow rw]
  (let [buckets (:buckets rw)
        cutoff (- (current-time-secs)
                  (* (:num-buckets rw)
                     (:bucket-size-secs rw)))
        to-remove (filter #(< % cutoff) (keys buckets))
        buckets (apply dissoc buckets to-remove)]
    (assoc rw :buckets buckets)
    ))

(defn rolling-window-size [^RollingWindow rw]
  (* (:bucket-size-secs rw) (:num-buckets rw)))

(defrecord RollingWindowSet [updater extractor windows all-time])

(defn rolling-window-set [updater merger extractor num-buckets & bucket-sizes]
  (RollingWindowSet. updater extractor (dofor [s bucket-sizes] (rolling-window updater merger extractor s num-buckets)) nil)
  )

(defn update-rolling-window-set
  ([^RollingWindowSet rws & args]
     (let [now (current-time-secs)
           new-windows (dofor [w (:windows rws)]
                         (apply update-rolling-window w now args))]
       (assoc rws :windows new-windows :all-time (apply (:updater rws) (:all-time rws) args))
       )))

(defn cleanup-rolling-window-set
  ([^RollingWindowSet rws]
     (let [windows (:windows rws)]
       (assoc rws :windows (map cleanup-rolling-window windows))
       )))

(defn value-rolling-window-set [^RollingWindowSet rws]
  (merge
   (into {}
         (for [w (:windows rws)]
           {(rolling-window-size w) (value-rolling-window w)}
           ))
   {:all-time ((:extractor rws) (:all-time rws))}))

(defn- incr-val
  ([amap key]
     (incr-val amap key 1))
  ([amap key amt]
     (let [val (get amap key (long 0))]
       (assoc amap key (+ val amt))
       )))

(defn- update-avg [curr val]
  (if curr
    [(+ (first curr) val) (inc (second curr))]
    [val (long 1)]
    ))

(defn- merge-avg [& avg]
  [(apply + (map first avg))
   (apply + (map second avg))
   ])

(defn- extract-avg [pair]
  (double (/ (first pair) (second pair))))

(defn- update-keyed-avg [amap key val]
  (assoc amap key (update-avg (get amap key) val)))

(defn- merge-keyed-avg [& vals]
  (apply merge-with merge-avg vals))

(defn- extract-keyed-avg [vals]
  (map-val extract-avg vals))

(defn- counter-extract [v]
  (if v v {}))

(defn keyed-counter-rolling-window-set [num-buckets & bucket-sizes]
  (apply rolling-window-set incr-val (partial merge-with +) counter-extract num-buckets bucket-sizes))

(defn avg-rolling-window-set [num-buckets & bucket-sizes]
  (apply rolling-window-set update-avg merge-avg extract-avg num-buckets bucket-sizes)
  )

(defn keyed-avg-rolling-window-set [num-buckets & bucket-sizes]
  (apply rolling-window-set update-keyed-avg merge-keyed-avg extract-keyed-avg num-buckets bucket-sizes))

;; (defn choose-bucket [val buckets]
;;   (let [ret (find-first #(<= val %) buckets)]
;;     (if ret
;;       ret
;;       (* 10 (first buckets)))
;;     ))

;; ;; buckets must be between 1 and 9
;; (defn to-proportional-bucket
;;   "Maps to a bucket in the values order of magnitude. So if buckets are [1 2 5],
;;    3 -> 5
;;    7 -> 10
;;    1234 -> 2000
;;    etc."
;;   [val buckets]
;;   (cond (= 0 val) 0
;;         (between? val 1 9) (choose-bucket val buckets)
;;         :else (* 10 (to-proportional-bucket (ceil (/ val 10))
;;                                             buckets))))

(def COMMON-FIELDS [:emitted :transferred])
(defrecord CommonStats [emitted transferred rate])

(def BOLT-FIELDS [:acked :failed :process-latencies :executed :execute-latencies])
;;acked and failed count individual tuples
(defrecord BoltExecutorStats [common acked failed process-latencies executed execute-latencies])

(def SPOUT-FIELDS [:acked :failed :complete-latencies])
;;acked and failed count tuple completion
(defrecord SpoutExecutorStats [common acked failed complete-latencies])

(def NUM-STAT-BUCKETS 20)
;; 10 minutes, 3 hours, 1 day
(def STAT-BUCKETS [30 540 4320])

(defn- mk-common-stats [rate]
  (CommonStats. (atom (apply keyed-counter-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
                (atom (apply keyed-counter-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
                rate
                ))

(defn mk-bolt-stats [rate]
  (BoltExecutorStats. (mk-common-stats rate)
                  (atom (apply keyed-counter-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
                  (atom (apply keyed-counter-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
                  (atom (apply keyed-avg-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
                  (atom (apply keyed-counter-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
                  (atom (apply keyed-avg-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
                  ))

(defn mk-spout-stats [rate]
  (SpoutExecutorStats. (mk-common-stats rate)
                   (atom (apply keyed-counter-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
                   (atom (apply keyed-counter-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
                   (atom (apply keyed-avg-rolling-window-set NUM-STAT-BUCKETS STAT-BUCKETS))
                   ))

(defmacro update-executor-stat! [stats path & args]
  (let [path (collectify path)]
    `(swap! (-> ~stats ~@path) update-rolling-window-set ~@args)
    ))

(defmacro stats-rate [stats]
  `(-> ~stats :common :rate))

(defn emitted-tuple! [stats stream]
  (update-executor-stat! stats [:common :emitted] stream (stats-rate stats)))

(defn transferred-tuples! [stats stream amt]
  (update-executor-stat! stats [:common :transferred] stream (* (stats-rate stats) amt)))

(defn bolt-execute-tuple! [^BoltExecutorStats stats component stream latency-ms]
  (let [key [component stream]]
    (update-executor-stat! stats :executed key (stats-rate stats))
    (update-executor-stat! stats :execute-latencies key latency-ms)
    ))

(defn bolt-acked-tuple! [^BoltExecutorStats stats component stream latency-ms]
  (let [key [component stream]]
    (update-executor-stat! stats :acked key (stats-rate stats))
    (update-executor-stat! stats :process-latencies key latency-ms)
    ))

(defn bolt-failed-tuple! [^BoltExecutorStats stats component stream latency-ms]
  (let [key [component stream]]
    (update-executor-stat! stats :failed key (stats-rate stats))
    ))

(defn spout-acked-tuple! [^SpoutExecutorStats stats stream latency-ms]
  (update-executor-stat! stats :acked stream (stats-rate stats))
  (update-executor-stat! stats :complete-latencies stream latency-ms)
  )

(defn spout-failed-tuple! [^SpoutExecutorStats stats stream latency-ms]
  (update-executor-stat! stats :failed stream (stats-rate stats))
  )

(defn- cleanup-stat! [stat]
  (swap! stat cleanup-rolling-window-set))

(defn- cleanup-common-stats! [^CommonStats stats]
  (doseq [f COMMON-FIELDS]
    (cleanup-stat! (f stats))
    ))

(defn cleanup-bolt-stats! [^BoltExecutorStats stats]
  (cleanup-common-stats! (:common stats))
  (doseq [f BOLT-FIELDS]
    (cleanup-stat! (f stats))
    ))

(defn cleanup-spout-stats! [^SpoutExecutorStats stats]
  (cleanup-common-stats! (:common stats))
  (doseq [f SPOUT-FIELDS]
    (cleanup-stat! (f stats))
    ))

(defn- value-stats [stats fields]
  (into
   {}
   (dofor [f fields]
          [f (value-rolling-window-set @(f stats))]
          )))

(defn- value-common-stats [^CommonStats stats]
  (merge
   (value-stats stats COMMON-FIELDS)
   {:rate (:rate stats)}))

(defn value-bolt-stats! [^BoltExecutorStats stats]
  (cleanup-bolt-stats! stats)
  (merge (value-common-stats (:common stats))
         (value-stats stats BOLT-FIELDS)
         {:type :bolt}))

(defn value-spout-stats! [^SpoutExecutorStats stats]
  (cleanup-spout-stats! stats)
  (merge (value-common-stats (:common stats))
         (value-stats stats SPOUT-FIELDS)
         {:type :spout}))


(defmulti render-stats! class-selector)

(defmethod render-stats! SpoutExecutorStats [stats]
  (value-spout-stats! stats))

(defmethod render-stats! BoltExecutorStats [stats]
  (value-bolt-stats! stats))

(defmulti thriftify-specific-stats :type)

(defn window-set-converter
  ([stats key-fn]
     ;; make the first key a string,
     (into {}
           (for [[k v] stats]
             [(str k)
              (into {}
                    (for [[k2 v2] v]
                      [(key-fn k2) v2]))]
             )
           ))
  ([stats]
     (window-set-converter stats identity)))

(defn to-global-stream-id [[component stream]]
  (GlobalStreamId. component stream)
  )

(defmethod thriftify-specific-stats :bolt
  [stats]
  (ExecutorSpecificStats/bolt
   (BoltStats. (window-set-converter (:acked stats) to-global-stream-id)
               (window-set-converter (:failed stats) to-global-stream-id)
               (window-set-converter (:process-latencies stats) to-global-stream-id)
               (window-set-converter (:executed stats) to-global-stream-id)
               (window-set-converter (:execute-latencies stats) to-global-stream-id)
               )))

(defmethod thriftify-specific-stats :spout
  [stats]
  (ExecutorSpecificStats/spout
   (SpoutStats. (window-set-converter (:acked stats))
                (window-set-converter (:failed stats))
                (window-set-converter (:complete-latencies stats)))
   ))

(defn thriftify-executor-stats [stats]
  (let [specific-stats (thriftify-specific-stats stats)]
    (ExecutorStats. (window-set-converter (:emitted stats))
                    (window-set-converter (:transferred stats))
                    specific-stats)))
