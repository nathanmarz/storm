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
  (:use [backtype.storm.util :only [clojurify-structure uuid defnk url-encode not-nil?]])
  (:use [clj-time coerce format])
  (:import [backtype.storm.generated ExecutorInfo ExecutorSummary])
  (:import [java.util EnumSet])
  (:import [org.eclipse.jetty.server Server]
           [org.eclipse.jetty.server.nio SelectChannelConnector]
           [org.eclipse.jetty.server.ssl SslSocketConnector]
           [org.eclipse.jetty.servlet ServletHolder FilterMapping]
	   [org.eclipse.jetty.util.ssl SslContextFactory]
           [org.eclipse.jetty.server DispatcherType]
           [org.eclipse.jetty.servlets CrossOriginFilter])
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

(defn- mk-ssl-connector [port ks-path ks-password ks-type key-password]
  (let [sslContextFactory (doto (SslContextFactory.)
                            (.setExcludeCipherSuites (into-array String ["SSL_RSA_WITH_RC4_128_MD5" "SSL_RSA_WITH_RC4_128_SHA"]))
                            (.setExcludeProtocols (into-array String ["SSLv3"]))
                            (.setAllowRenegotiate false)
                            (.setKeyStorePath ks-path)
                            (.setKeyStoreType ks-type)
                            (.setKeyStorePassword ks-password)
                            (.setKeyManagerPassword key-password))]
    (doto (SslSocketConnector. sslContextFactory)
      (.setPort port))))


(defn config-ssl [server port ks-path ks-password ks-type key-password]
  (when (> port 0)
    (.addConnector server (mk-ssl-connector port ks-path ks-password ks-type key-password))))

(defn cors-filter-handler
  []
  (doto (org.eclipse.jetty.servlet.FilterHolder. (CrossOriginFilter.))
    (.setInitParameter CrossOriginFilter/ALLOWED_ORIGINS_PARAM "*")
    (.setInitParameter CrossOriginFilter/ALLOWED_METHODS_PARAM "GET, POST, PUT")
    (.setInitParameter CrossOriginFilter/ALLOWED_HEADERS_PARAM "X-Requested-With, X-Requested-By, Access-Control-Allow-Origin, Content-Type, Content-Length, Accept, Origin")
    (.setInitParameter CrossOriginFilter/ACCESS_CONTROL_ALLOW_ORIGIN_HEADER "*")
    ))

(defn config-filter [server handler filters-confs]
  (if filters-confs
    (let [servlet-holder (ServletHolder.
                           (ring.util.servlet/servlet handler))
          context (doto (org.eclipse.jetty.servlet.ServletContextHandler. server "/")
                    (.addServlet servlet-holder "/"))]
      (.addFilter context (cors-filter-handler) "/*" (EnumSet/allOf DispatcherType))
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

(defn- remove-non-ssl-connectors [server]
  (doseq [c (.getConnectors server)]
    (when-not (or (nil? c) (instance? SslSocketConnector c))
      (.removeConnector server c)
      ))
  server)

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
                    (.setSendDateHeader true))
        https-port (options :https-port)]
    (if (and (not-nil? https-port) (> https-port 0)) (remove-non-ssl-connectors server))
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
