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
(ns backtype.storm.daemon.logviewer
  (:use compojure.core)
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util log])
  (:use [ring.adapter.jetty :only [run-jetty]])
  (:import [org.slf4j LoggerFactory])
  (:import [ch.qos.logback.classic Logger])
  (:import [org.apache.commons.logging LogFactory])
  (:import [org.apache.commons.logging.impl Log4JLogger])
  (:import [ch.qos.logback.core FileAppender])
  (:import [org.apache.log4j Level])
  (:import [java.io File])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [clojure.string :as string])
  (:gen-class))

(defn tail-file [path tail]
  (let [flen (.length (clojure.java.io/file path))
        skip (- flen tail)]
    (with-open [input (clojure.java.io/input-stream path)
                output (java.io.ByteArrayOutputStream.)]
      (if (> skip 0) (.skip input skip))
      (let [buffer (make-array Byte/TYPE 1024)]
        (loop []
          (let [size (.read input buffer)]
            (when (and (pos? size) (< (.size output) tail))
              (do (.write output buffer 0 size)
                  (recur))))))
      (.toString output))
    ))

(defn log-root-dir
  "Given an appender name, as configured, get the parent directory of the appender's log file.

Note that if anything goes wrong, this will throw an Error and exit."
  [appender-name]
  (let [appender (.getAppender (LoggerFactory/getLogger Logger/ROOT_LOGGER_NAME) appender-name)]
    (if (and appender-name appender (instance? FileAppender appender))
      (.getParent (File. (.getFile appender)))
      (throw
       (RuntimeException. "Log viewer could not find configured appender, or the appender is not a FileAppender. Please check that the appender name configured in storm and logback agree.")))))

(defn log-page [file tail grep root-dir]
  (let [path (.getCanonicalPath (File. root-dir file))
        tail (if tail
               (min 10485760 (Integer/parseInt tail))
               10240)
        tail-string (tail-file path tail)]
    (if grep
       (clojure.string/join "\n<br>"
         (filter #(.contains % grep) (.split tail-string "\n")))
       (.replaceAll tail-string "\n" "\n<br>"))))

(defn log-level-page [name level]
  (let [log (LogFactory/getLog name)]
    (if level
      (if (instance? Log4JLogger log)
        (.setLevel (.getLogger log) (Level/toLevel level))))
    (str "effective log level for " name " is " (.getLevel (.getLogger log)))))

(defn log-template [body]
  (html4
   [:head
    [:title "Storm log viewer"]
    (include-css "/css/bootstrap-1.4.0.css")
    (include-css "/css/style.css")
    (include-js "/js/jquery-1.6.2.min.js")
    (include-js "/js/jquery.tablesorter.min.js")
    (include-js "/js/jquery.cookies.2.2.0.min.js")
    (include-js "/js/script.js")
    ]
   [:body
    (seq body)
    ]))

(defroutes log-routes
  (GET "/log" [:as req & m]
       (log-template (log-page (:file m) (:tail m) (:grep m) (:log-root req))))
  (GET "/loglevel" [:as {cookies :cookies} & m]
       (log-template (log-level-page (:name m) (:level m))))
  (route/resources "/")
  (route/not-found "Page not found"))

(def logapp
  (handler/site log-routes)
 )

(defn conf-middleware
  "For passing the storm configuration with each request."
  [app log-root]
  (fn [req]
    (app (assoc req :log-root log-root))))

(defn start-logviewer [port log-root]
  (run-jetty (conf-middleware logapp log-root) {:port port}))

(defn -main []
  (let [conf (read-storm-config)
        log-root (log-root-dir (conf LOGVIEWER-APPENDER-NAME))]
    (start-logviewer (int (conf LOGVIEWER-PORT)) log-root)))
