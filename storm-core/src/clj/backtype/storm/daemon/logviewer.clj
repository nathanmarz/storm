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
  (:use [backtype.storm.ui helpers])
  (:use [ring.adapter.jetty :only [run-jetty]])
  (:import [org.slf4j LoggerFactory])
  (:import [ch.qos.logback.classic Logger])
  (:import [ch.qos.logback.core FileAppender])
  (:import [java.io File FileInputStream])
  (:import [backtype.storm.ui InvalidRequestException])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.middleware.keyword-params]
            [ring.util.response :as resp]
            [clojure.string :as string])
  (:gen-class))

(defn page-file
  ([path tail]
    (let [flen (.length (clojure.java.io/file path))
          skip (- flen tail)]
      (page-file path skip tail)))
  ([path start length]
    (with-open [input (FileInputStream. path)
                output (java.io.ByteArrayOutputStream.)]
      (if (>= start (.length (clojure.java.io/file path)))
        (throw
          (InvalidRequestException. "Cannot start past the end of the file")))
      (if (> start 0)
        ;; FileInputStream#skip may not work the first time.
        (loop [skipped 0]
          (let [skipped (+ skipped (.skip input (- start skipped)))]
            (if (< skipped start) (recur skipped)))))
      (let [buffer (make-array Byte/TYPE 1024)]
        (loop []
          (when (< (.size output) length)
            (let [size (.read input buffer 0 (min 1024 (- length (.size output))))]
              (when (pos? size)
                (.write output buffer 0 size)
                (recur)))))
      (.toString output)))))

(defn log-root-dir
  "Given an appender name, as configured, get the parent directory of the appender's log file.

Note that if anything goes wrong, this will throw an Error and exit."
  [appender-name]
  (let [appender (.getAppender (LoggerFactory/getLogger Logger/ROOT_LOGGER_NAME) appender-name)]
    (if (and appender-name appender (instance? FileAppender appender))
      (.getParent (File. (.getFile appender)))
      (throw
       (RuntimeException. "Log viewer could not find configured appender, or the appender is not a FileAppender. Please check that the appender name configured in storm and logback agree.")))))

(defn pager-links [fname start length file-size]
  (let [prev-start (max 0 (- start length))
        next-start (if (> file-size 0)
                     (min (max 0 (- file-size length)) (+ start length))
                     (+ start length))]
    [[:div.pagination
      [:ul
        (concat
          [[(if (< prev-start start) (keyword "li") (keyword "li.disabled"))
            (link-to (url "/log"
                          {:file fname
                             :start (max 0 (- start length))
                             :length length})
                     "Prev")]]
          [[:li (link-to
                  (url "/log"
                       {:file fname
                        :start 0
                        :length length})
                  "First")]]
          [[:li (link-to
                  (url "/log"
                       {:file fname
                        :length length})
                  "Last")]]
          [[(if (> next-start start) (keyword "li.next") (keyword "li.next.disabled"))
            (link-to (url "/log"
                          {:file fname
                           :start (min (max 0 (- file-size length))
                                       (+ start length))
                           :length length})
                     "Next")]])]]]))

(defn- download-link [fname]
  [[:p (link-to (url-format "/download/%s" fname) "Download Full Log")]])

(defn log-page [fname start length grep root-dir]
  (let [file (.getCanonicalFile (File. root-dir fname))
        file-length (.length file)
        path (.getCanonicalPath file)]
    (if (= (File. root-dir)
           (.getParentFile file))
      (let [default-length 51200
            length (if length
                     (min 10485760 length)
                   default-length)
            log-string (escape-html
                         (if start
                           (page-file path start length)
                           (page-file path length)))
            start (or start (- file-length length))]
        (if grep
          (html [:pre#logContent
                 (if grep
                   (filter #(.contains % grep)
                           (.split log-string "\n"))
                   log-string)])
          (let [pager-data (pager-links fname start length file-length)]
            (html (concat pager-data
                          (download-link fname)
                          [[:pre#logContent log-string]]
                          pager-data)))))
      (-> (resp/response "Page not found")
          (resp/status 404)))))

(defn download-log-file [fname req resp log-root]
  (let [file (.getCanonicalFile (File. log-root fname))
        path (.getCanonicalPath file)]
    (if (= (File. log-root) (.getParentFile file))
      (-> (resp/response file)
          (resp/content-type "application/octet-stream"))
      (-> (resp/response "Page not found")
          (resp/status 404)))))

(defn log-template
  ([body] (log-template body nil))
  ([body fname]
    (html4
     [:head
      [:title (str (escape-html fname) " - Storm Log Viewer")]
      (include-css "/css/bootstrap-1.4.0.css")
      (include-css "/css/style.css")
      ]
     [:body
      (concat
        [[:h3 (escape-html fname)]]
        (seq body))
      ])))

(defn- parse-long-from-map [m k]
  (try
    (Long/parseLong (k m))
    (catch NumberFormatException ex
      (throw (InvalidRequestException.
               (str "Could not make an integer out of the query parameter '"
                    (name k) "'")
               ex)))))

(defroutes log-routes
  (GET "/log" [:as req & m]
       (try
         (let [start (if (:start m) (parse-long-from-map m :start))
               length (if (:length m) (parse-long-from-map m :length))
               root-dir (:log-root req)]
           (log-template (log-page (:file m) start length (:grep m) root-dir)
                         (:file m)))
         (catch InvalidRequestException ex
           (log-error ex)
           (ring-response-from-exception ex))))
  (GET "/download/:file" [:as {:keys [servlet-request servlet-response]} file & m]
       (download-log-file file servlet-request servlet-response (:log-root m)))
  (route/resources "/")
  (route/not-found "Page not found"))

(def logapp
  (handler/api log-routes)
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
