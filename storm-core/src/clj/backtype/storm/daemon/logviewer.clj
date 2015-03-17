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
  (:use [clojure.set :only [difference intersection]])
  (:use [clojure.string :only [blank?]])
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util log timer])
  (:use [backtype.storm.ui helpers])
  (:import [org.slf4j LoggerFactory])
  (:import [ch.qos.logback.classic Logger])
  (:import [ch.qos.logback.core FileAppender])
  (:import [java.io File FileFilter FileInputStream])
  (:import [org.yaml.snakeyaml Yaml]
           [org.yaml.snakeyaml.constructor SafeConstructor])
  (:import [backtype.storm.ui InvalidRequestException]
           [backtype.storm.security.auth AuthUtils])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.middleware.keyword-params]
            [ring.util.response :as resp])
  (:require [backtype.storm.daemon common [supervisor :as supervisor]])
  (:import [java.io File FileFilter])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.util.response :as resp]
            [clojure.string :as string])
  (:gen-class))

(def ^:dynamic *STORM-CONF* (read-storm-config))

(defn cleanup-cutoff-age-millis [conf now-millis]
  (- now-millis (* (conf LOGVIEWER-CLEANUP-AGE-MINS) 60 1000)))

(defn mk-FileFilter-for-log-cleanup [conf now-millis]
  (let [cutoff-age-millis (cleanup-cutoff-age-millis conf now-millis)]
    (reify FileFilter (^boolean accept [this ^File file]
                        (boolean (and
                          (.isFile file)
                          (re-find worker-log-filename-pattern (.getName file))
                          (<= (.lastModified file) cutoff-age-millis)))))))

(defn select-files-for-cleanup [conf now-millis root-dir]
  (let [file-filter (mk-FileFilter-for-log-cleanup conf now-millis)]
    (.listFiles (File. root-dir) file-filter)))

(defn get-metadata-file-for-log-root-name [root-name root-dir]
  (let [metaFile (clojure.java.io/file root-dir "metadata"
                                       (str root-name ".yaml"))]
    (if (.exists metaFile)
      metaFile
      (do
        (log-warn "Could not find " (.getCanonicalPath metaFile)
                  " to clean up for " root-name)
        nil))))

(defn get-worker-id-from-metadata-file [metaFile]
  (get (clojure-from-yaml-file metaFile) "worker-id"))

(defn get-topo-owner-from-metadata-file [metaFile]
  (get (clojure-from-yaml-file metaFile) TOPOLOGY-SUBMITTER-USER))

(defn get-log-root->files-map [log-files]
  "Returns a map of \"root name\" to a the set of files in log-files having the
  root name.  The \"root name\" of a log file is the part of the name preceding
  the extension."
  (reduce #(assoc %1                                      ;; The accumulated map so far
                  (first %2)                              ;; key: The root name of the log file
                  (conj (%1 (first %2) #{}) (second %2))) ;; val: The set of log files with the root name
          {}                                              ;; initial (empty) map
          (map #(list
                  (second (re-find worker-log-filename-pattern (.getName %))) ;; The root name of the log file
                  %)                                                          ;; The log file
               log-files)))

(defn identify-worker-log-files [log-files root-dir]
  (into {} (for [log-root-entry (get-log-root->files-map log-files)
                 :let [metaFile (get-metadata-file-for-log-root-name
                                  (key log-root-entry) root-dir)
                       log-root (key log-root-entry)
                       files (val log-root-entry)]
                 :when metaFile]
             {(get-worker-id-from-metadata-file metaFile)
              {:owner (get-topo-owner-from-metadata-file metaFile)
               :files
                 ;; If each log for this root name is to be deleted, then
                 ;; include the metadata file also.
                 (if (empty? (difference
                                  (set (filter #(re-find (re-pattern log-root) %)
                                               (read-dir-contents root-dir)))
                                  (set (map #(.getName %) files))))
                  (conj files metaFile)
                  ;; Otherwise, keep the list of files as it is.
                  files)}})))

(defn get-dead-worker-files-and-owners [conf now-secs log-files root-dir]
  (if (empty? log-files)
    {}
    (let [id->heartbeat (supervisor/read-worker-heartbeats conf)
          alive-ids (keys (remove
                            #(or (not (val %))
                                 (supervisor/is-worker-hb-timed-out? now-secs (val %) conf))
                            id->heartbeat))
          id->entries (identify-worker-log-files log-files root-dir)]
      (for [[id {:keys [owner files]}] id->entries
            :when (not (contains? (set alive-ids) id))]
        {:owner owner
         :files files}))))

(defn cleanup-fn! [log-root-dir]
  (let [now-secs (current-time-secs)
        old-log-files (select-files-for-cleanup *STORM-CONF* (* now-secs 1000) log-root-dir)
        dead-worker-files (get-dead-worker-files-and-owners *STORM-CONF* now-secs old-log-files log-root-dir)]
    (log-debug "log cleanup: now=" now-secs
               " old log files " (pr-str (map #(.getName %) old-log-files))
               " dead worker files " (->> dead-worker-files
                                          (mapcat (fn [{l :files}] l))
                                          (map #(.getName %))
                                          (pr-str)))
    (dofor [{:keys [owner files]} dead-worker-files
            file files]
      (let [path (.getCanonicalPath file)]
        (log-message "Cleaning up: Removing " path)
        (try
          (if (or (blank? owner) (re-matches #".*\.yaml$" path))
            (rmr path)
            ;; worker-launcher does not actually launch a worker process.  It
            ;; merely executes one of a prescribed set of commands.  In this case, we ask it
            ;; to delete a file as the owner of that file.
            (supervisor/worker-launcher *STORM-CONF* owner (str "rmr " path)))
          (catch Exception ex
            (log-error ex)))))))

(defn start-log-cleaner! [conf log-root-dir]
  (let [interval-secs (conf LOGVIEWER-CLEANUP-INTERVAL-SECS)]
    (when interval-secs
      (log-debug "starting log cleanup thread at interval: " interval-secs)
      (schedule-recurring (mk-timer :thread-name "logviewer-cleanup")
                          0 ;; Start immediately.
                          interval-secs
                          (fn [] (cleanup-fn! log-root-dir))))))

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

(defn get-log-user-group-whitelist [fname]
  (let [wl-file (get-log-metadata-file fname)
        m (clojure-from-yaml-file wl-file)
        user-wl (.get m LOGS-USERS)
        user-wl (if user-wl user-wl [])
        group-wl (.get m LOGS-GROUPS)
        group-wl (if group-wl group-wl [])]
    [user-wl group-wl]))

(def igroup-mapper (AuthUtils/GetGroupMappingServiceProviderPlugin *STORM-CONF*))
(defn user-groups
  [user]
  (if (blank? user) [] (.getGroups igroup-mapper user)))

(defn authorized-log-user? [user fname conf]
  (if (or (blank? user) (blank? fname))
    nil
    (let [groups (user-groups user)
          [user-wl group-wl] (get-log-user-group-whitelist fname)
          logs-users (concat (conf LOGS-USERS)
                             (conf NIMBUS-ADMINS)
                             user-wl)
          logs-groups (concat (conf LOGS-GROUPS)
                              group-wl)]
       (or (some #(= % user) logs-users)
           (< 0 (.size (intersection (set groups) (set group-wl))))))))

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

(defn log-page [fname start length grep user root-dir]
  (if (or (blank? (*STORM-CONF* UI-FILTER))
          (authorized-log-user? user fname *STORM-CONF*))
    (let [file (.getCanonicalFile (File. root-dir fname))
          file-length (.length file)
          path (.getCanonicalPath file)]
      (if (and (= (.getCanonicalFile (File. root-dir))
                  (.getParentFile file))
               (.exists file))
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
                     (->> (.split log-string "\n")
                          (filter #(.contains % grep))
                          (string/join "\n"))
                     log-string)])
            (let [pager-data (pager-links fname start length file-length)]
              (html (concat pager-data
                            (download-link fname)
                            [[:pre#logContent log-string]]
                            pager-data)))))
        (-> (resp/response "Page not found")
            (resp/status 404))))
    (unauthorized-user-html user)))

(defn download-log-file [fname req resp user ^String root-dir]
  (let [file (.getCanonicalFile (File. root-dir fname))]
    (if (= (File. root-dir) (.getParentFile file))
      (if (or (blank? (*STORM-CONF* UI-FILTER))
              (authorized-log-user? user fname *STORM-CONF*))
        (-> (resp/response file)
            (resp/content-type "application/octet-stream"))
        (unauthorized-user-html user))
      (-> (resp/response "Page not found")
          (resp/status 404)))))

(defn log-template
  ([body] (log-template body nil nil))
  ([body fname user]
    (html4
     [:head
      [:title (str (escape-html fname) " - Storm Log Viewer")]
      (include-css "/css/bootstrap-1.4.0.css")
      (include-css "/css/style.css")
      ]
     [:body
      (concat
        (when (not (blank? user)) [[:div.ui-user [:p "User: " user]]])
        [[:h3 (escape-html fname)]]
        (seq body))
      ])))

(def http-creds-handler (AuthUtils/GetUiHttpCredentialsPlugin *STORM-CONF*))

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
         (let [servlet-request (:servlet-request req)
               log-root (:log-root req)
               user (.getUserName http-creds-handler servlet-request)
               start (if (:start m) (parse-long-from-map m :start))
               length (if (:length m) (parse-long-from-map m :length))]
           (log-template (log-page (:file m) start length (:grep m) user log-root)
                         (:file m) user))
         (catch InvalidRequestException ex
           (log-error ex)
           (ring-response-from-exception ex))))
  (GET "/download/:file" [:as {:keys [servlet-request servlet-response log-root]} file & m]
       (try
         (let [user (.getUserName http-creds-handler servlet-request)]
           (download-log-file file servlet-request servlet-response user log-root))
         (catch InvalidRequestException ex
           (log-error ex)
           (ring-response-from-exception ex))))
  (route/resources "/")
  (route/not-found "Page not found"))

(defn conf-middleware
  "For passing the storm configuration with each request."
  [app log-root]
  (fn [req]
    (app (assoc req :log-root log-root))))

(defn start-logviewer! [conf log-root-dir]
  (try
    (let [header-buffer-size (int (.get conf UI-HEADER-BUFFER-BYTES))
          filter-class (conf UI-FILTER)
          filter-params (conf UI-FILTER-PARAMS)
          logapp (handler/api log-routes) ;; query params as map
          middle (conf-middleware logapp log-root-dir)
          filters-confs (if (conf UI-FILTER)
                          [{:filter-class filter-class
                            :filter-params (or (conf UI-FILTER-PARAMS) {})}]
                          [])
          filters-confs (concat filters-confs
                          [{:filter-class "org.eclipse.jetty.servlets.GzipFilter"
                            :filter-name "Gzipper"
                            :filter-params {}}])]
      (storm-run-jetty {:port (int (conf LOGVIEWER-PORT))
                        :configurator (fn [server]
                                        (config-filter server middle filters-confs))}))
  (catch Exception ex
    (log-error ex))))

(defn -main []
  (let [conf (read-storm-config)
        log-root (log-root-dir (conf LOGVIEWER-APPENDER-NAME))]
    (setup-default-uncaught-exception-handler)
    (start-log-cleaner! conf log-root)
    (start-logviewer! conf log-root)))
