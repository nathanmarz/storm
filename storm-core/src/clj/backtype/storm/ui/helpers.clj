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
(ns backtype.storm.ui.helpers
  (:use compojure.core)
  (:use [hiccup core page-helpers])
  (:use [clojure
         [string :only [blank? join]]
         [walk :only [keywordize-keys]]])
  (:use [backtype.storm config log])
  (:use [backtype.storm.util :only [clojurify-structure uuid defnk url-encode]])
  (:use [clj-time coerce format])
  (:import [backtype.storm.generated ExecutorInfo ExecutorSummary])
  (:import [org.eclipse.jetty.server Server]
           [org.eclipse.jetty.server.nio SelectChannelConnector]
           [org.eclipse.jetty.server.ssl SslSocketConnector]
           [org.eclipse.jetty.servlet ServletHolder FilterMapping])
  (:require [ring.util servlet])
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


(defelem table [headers-map data]
  [:table
   [:thead
    [:tr
     (for [h headers-map]
       [:th (if (:text h) [:span (:attr h) (:text h)] h)])
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

(defn url-format [fmt & args]
  (String/format fmt
    (to-array (map #(url-encode (str %)) args))))

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

(defn unauthorized-user-html [user]
  [[:h2 "User '" (escape-html user) "' is not authorized."]])

(defn- mk-ssl-connector [port ks-path ks-password ks-type]
  (doto (SslSocketConnector.)
    (.setExcludeCipherSuites (into-array String ["SSL_RSA_WITH_RC4_128_MD5" "SSL_RSA_WITH_RC4_128_SHA"]))
    (.setAllowRenegotiate false)
    (.setKeystore ks-path)
    (.setKeystoreType ks-type)
    (.setKeyPassword ks-password)
    (.setPassword ks-password)
    (.setPort port)))

(defn config-ssl [server port ks-path ks-password ks-type]
  (when (> port 0)
    (.addConnector server (mk-ssl-connector port ks-path ks-password ks-type))))

(defn config-filter [server handler filters-confs]
  (if filters-confs
    (let [servlet-holder (ServletHolder.
                           (ring.util.servlet/servlet handler))
          context (doto (org.eclipse.jetty.servlet.ServletContextHandler. server "/")
                    (.addServlet servlet-holder "/"))]
      (doseq [{:keys [filter-name filter-class filter-params]} filters-confs]
        (if filter-class
          (let [filter-holder (doto (org.eclipse.jetty.servlet.FilterHolder.)
                                (.setClassName filter-class)
                                (.setName (or filter-name filter-class))
                                (.setInitParameters (or filter-params {})))]
            (.addFilter context filter-holder "/*" FilterMapping/ALL))))
      (.setHandler server context))))

(defn ring-response-from-exception [ex]
  {:headers {}
   :status 400
   :body (.getMessage ex)})

;; Modified from ring.adapter.jetty 1.3.0
(defn- jetty-create-server
  "Construct a Jetty Server instance."
  [options]
  (let [connector (doto (SelectChannelConnector.)
                    (.setPort (options :port 80))
                    (.setHost (options :host))
                    (.setMaxIdleTime (options :max-idle-time 200000)))
        server    (doto (Server.)
                    (.addConnector connector)
                    (.setSendDateHeader true))]
    server))

(defn storm-run-jetty
  "Modified version of run-jetty
  Assumes configurator sets handler."
  [config]
  {:pre [(:configurator config)]}
  (let [#^Server s (jetty-create-server (dissoc config :configurator))
        configurator (:configurator config)]
    (configurator s)
    (.start s)))
