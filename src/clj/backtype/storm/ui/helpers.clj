(ns backtype.storm.ui.helpers
  (:use compojure.core)
  (:use [hiccup core page-helpers])
  (:use [clojure [string :only [join]]])
  (:use [backtype.storm.util :only [uuid defnk]])
  (:use [clj-time coerce format])
  (:import [backtype.storm.generated ExecutorInfo ExecutorSummary])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]))

(defn split-divide [val divider]
  [(Integer. (int (/ val divider))) (mod val divider)]
  )

(def PRETTY-SEC-DIVIDERS
     [["s" 60]
      ["m" 60]
      ["h" 24]
      ["d" nil]])

(def PRETTY-MS-DIVIDERS
     (cons ["ms" 1000]
           PRETTY-SEC-DIVIDERS))

(defn pretty-uptime-str* [val dividers]
  (let [val (if (string? val) (Integer/parseInt val) val)
        vals (reduce (fn [[state val] [_ divider]]
                       (if (pos? val)
                         (let [[divided mod] (if divider
                                               (split-divide val divider)
                                               [nil val])]
                           [(concat state [mod])
                            divided]
                           )
                         [state val]
                         ))
                     [[] val]
                     dividers)
        strs (->>
              (first vals)
              (map
               (fn [[suffix _] val]
                 (str val suffix))
               dividers
               ))]
    (join " " (reverse strs))
    ))

(defn pretty-uptime-sec [secs]
  (pretty-uptime-str* secs PRETTY-SEC-DIVIDERS))

(defn pretty-uptime-ms [ms]
  (pretty-uptime-str* ms PRETTY-MS-DIVIDERS))


(defelem table [headers data]
  [:table
   [:thead
    [:tr
     (for [h headers]
       [:th h])
     ]]
   [:tbody
    (for [row data]
      [:tr
       (for [col row]
         [:td col]
         )]
      )]
   ])

(defnk sort-table [id :sort-list "[[0,0]]" :time-cols []]
  (let [strs (for [c time-cols] (format "%s: { sorter: 'stormtimestr'}" c))
        sorters (join ", " strs)]
    [:script
     (format  "$(document).ready(function() {
$(\"table#%s\").each(function(i) { $(this).tablesorter({ sortList: %s, headers: {%s}}); });
});"
              id
              sort-list
              sorters)]))

(defn float-str [n]
  (if n
    (format "%.3f" (float n))
    "0"
    ))

(defn swap-map-order [m]
  (->> m
       (map (fn [[k v]]
              (into
               {}
               (for [[k2 v2] v]
                 [k2 {k v2}]
                 ))
              ))
       (apply merge-with merge)
       ))

(defn sorted-table [headers data & args]
  (let [id (uuid)]
    (concat
     [(table {:class "zebra-striped" :id id}
             headers
             data)]
     (if-not (empty? data)
       [(apply sort-table id args)])
     )))

(defn date-str [secs]
  (let [dt (from-long (* 1000 (long secs)))]
    (unparse (:rfc822 formatters) dt)
    ))

(defn url-format [fmt & args]
  (String/format fmt 
    (to-array (map #(java.net.URLEncoder/encode (str %)) args))))

(defn to-tasks [^ExecutorInfo e]
  (let [start (.get_task_start e)
        end (.get_task_end e)]
    (range start (inc end))
    ))

(defn sum-tasks [executors]
  (reduce + (->> executors
                 (map #(.get_executor_info ^ExecutorSummary %))
                 (map to-tasks)
                 (map count))))

(defn pretty-executor-info [^ExecutorInfo e]
  (str "[" (.get_task_start e) "-" (.get_task_end e) "]"))

