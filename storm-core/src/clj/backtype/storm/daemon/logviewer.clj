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
  (:use [clojure.string :only [blank? split]])
  (:use [hiccup core page-helpers form-helpers])
  (:use [backtype.storm config util log timer])
  (:use [backtype.storm.ui helpers])
  (:import [backtype.storm.utils Utils])
  (:import [org.slf4j LoggerFactory])
  (:import [java.io File FileFilter FileInputStream InputStream])
  (:import [java.util.zip GZIPInputStream])
  (:import [org.apache.logging.log4j LogManager])
  (:import [org.apache.logging.log4j.core Appender LoggerContext])
  (:import [org.apache.logging.log4j.core.appender RollingFileAppender])
  (:import [org.yaml.snakeyaml Yaml]
           [org.yaml.snakeyaml.constructor SafeConstructor])
  (:import [backtype.storm.ui InvalidRequestException]
           [backtype.storm.security.auth AuthUtils])
  (:require [backtype.storm.daemon common [supervisor :as supervisor]])
  (:import [java.io File FileFilter])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.middleware.keyword-params]
            [ring.util.codec :as codec]
            [ring.util.response :as resp]
            [clojure.string :as string])
  (:gen-class))

(def ^:dynamic *STORM-CONF* (read-storm-config))

(defn cleanup-cutoff-age-millis [conf now-millis]
  (- now-millis (* (conf LOGVIEWER-CLEANUP-AGE-MINS) 60 1000)))

(defn- last-modifiedtime-worker-logdir
  "Return the last modified time for all log files in a worker's log dir"
  [log-dir]
  (apply max
         (.lastModified log-dir)
         (for [^File file (.listFiles log-dir)]
           (.lastModified file))))

(defn mk-FileFilter-for-log-cleanup [conf now-millis]
  (let [cutoff-age-millis (cleanup-cutoff-age-millis conf now-millis)]
    (reify FileFilter (^boolean accept [this ^File file]
                        (boolean (and
                                   (not (.isFile file))
                                   (<= (last-modifiedtime-worker-logdir file) cutoff-age-millis)))))))

(defn select-dirs-for-cleanup [conf now-millis root-dir]
  (let [file-filter (mk-FileFilter-for-log-cleanup conf now-millis)]
    (reduce clojure.set/union
            (sorted-set)
            (for [^File topo-dir (.listFiles (File. root-dir))]
              (into [] (.listFiles topo-dir file-filter))))))

(defn get-topo-port-workerlog
  "Return the path of the worker log with the format of topoId/port/worker.log.*"
  [^File file]
  (clojure.string/join file-path-separator
                       (take-last 3
                                  (split (.getCanonicalPath file) (re-pattern file-path-separator)))))

(defn get-metadata-file-for-log-root-name [root-name root-dir]
  (let [metaFile (clojure.java.io/file root-dir "metadata"
                                       (str root-name ".yaml"))]
    (if (.exists metaFile)
      metaFile
      (do
        (log-warn "Could not find " (.getCanonicalPath metaFile)
                  " to clean up for " root-name)
        nil))))

(defn get-metadata-file-for-wroker-logdir [logdir]
  (let [metaFile (clojure.java.io/file logdir "worker.yaml")]
    (if (.exists metaFile)
      metaFile
      (do
        (log-warn "Could not find " (.getCanonicalPath metaFile)
                  " to clean up for " logdir)
        nil))))

(defn get-worker-id-from-metadata-file [metaFile]
  (get (clojure-from-yaml-file metaFile) "worker-id"))

(defn get-topo-owner-from-metadata-file [metaFile]
  (get (clojure-from-yaml-file metaFile) TOPOLOGY-SUBMITTER-USER))

(defn identify-worker-log-dirs [log-dirs]
  "return the workerid to worker-log-dir map"
  (into {} (for [logdir log-dirs
                 :let [metaFile (get-metadata-file-for-wroker-logdir logdir)]
                 :when metaFile]
             {(get-worker-id-from-metadata-file metaFile) logdir})))

(defn get-alive-ids
  [conf now-secs]
  (->>
    (supervisor/read-worker-heartbeats conf)
    (remove
      #(or (not (val %))
           (supervisor/is-worker-hb-timed-out? now-secs
                                               (val %)
                                               conf)))
    keys
    set))

(defn get-dead-worker-dirs
  "Return a sorted set of java.io.Files that were written by workers that are
  now dead"
  [conf now-secs log-dirs]
  (if (empty? log-dirs)
    (sorted-set)
    (let [alive-ids (get-alive-ids conf now-secs)
          id->dir (identify-worker-log-dirs log-dirs)]
      (apply sorted-set
             (for [[id dir] id->dir
                   :when (not (contains? alive-ids id))]
               dir)))))

(defn get-all-worker-dirs [^File root-dir]
  (reduce clojure.set/union
          (sorted-set)
          (for [^File topo-dir (.listFiles root-dir)]
            (into [] (.listFiles topo-dir)))))

(defn get-alive-worker-dirs
  "Return a sorted set of java.io.Files that were written by workers that are
  now active"
  [conf root-dir]
  (let [alive-ids (get-alive-ids conf (current-time-secs))
        log-dirs (get-all-worker-dirs root-dir)
        id->dir (identify-worker-log-dirs log-dirs)]
    (apply sorted-set
           (for [[id dir] id->dir
                 :when (contains? alive-ids id)]
             (.getCanonicalPath dir)))))

(defn get-all-logs-for-rootdir [^File log-dir]
  (reduce concat
          (for [port-dir (get-all-worker-dirs log-dir)]
            (into [] (.listFiles port-dir)))))

(defn is-active-log [^File file]
  (re-find #"\.(log|err|out|current|yaml|pid)$" (.getName file)))

(defn filter-candidate-files
  "Filter candidate files for global cleanup"
  [logs log-dir]
  (let [alive-worker-dirs (get-alive-worker-dirs *STORM-CONF* log-dir)]
    (filter #(and (not= (.getName %) "worker.yaml")  ; exclude metadata file
                  (not (and (contains? alive-worker-dirs (.getCanonicalPath (.getParentFile %)))
                            (is-active-log %)))) ; exclude active workers' active logs
            logs)))

(defn sorted-worker-logs
  "Collect the wroker log files recursively, sorted by decreasing age."
  [^File root-dir]
  (let [files (get-all-logs-for-rootdir root-dir)
        logs (filter-candidate-files files root-dir)]
    (sort-by #(.lastModified %) logs)))

(defn sum-file-size
  "Given a sequence of Files, sum their sizes."
  [files]
  (reduce #(+ %1 (.length %2)) 0 files))

(defn delete-oldest-while-logs-too-large [logs_ size]
  (loop [logs logs_]
    (if (> (sum-file-size logs) size)
      (do
        (log-message "Log sizes too high. Going to delete: " (.getName (first logs)))
        (try (rmr (.getCanonicalPath (first logs)))
             (catch Exception ex (log-error ex)))
        (recur (rest logs)))
      logs)))

(defn per-workerdir-cleanup
  "Delete the oldest files in overloaded worker log dir"
  [^File root-dir size]
  (dofor [worker-dir (get-all-worker-dirs root-dir)]
    (let [filtered-logs (filter #(not (is-active-log %)) (.listFiles worker-dir))
          sorted-logs (sort-by #(.lastModified %) filtered-logs)]
      (delete-oldest-while-logs-too-large sorted-logs size))))

(defn cleanup-empty-topodir
  "Delete the topo dir if it contains zero port dirs"
  [^File dir]
  (let [topodir (.getParentFile dir)]
    (if (empty? (.listFiles topodir))
      (rmr (.getCanonicalPath topodir)))))

(defn cleanup-fn!
  "Delete old log dirs for which the workers are no longer alive"
  [log-root-dir]
  (let [now-secs (current-time-secs)
        old-log-dirs (select-dirs-for-cleanup *STORM-CONF*
                                              (* now-secs 1000)
                                              log-root-dir)
        total-size (*STORM-CONF* LOGVIEWER-MAX-SUM-WORKER-LOGS-SIZE-MB)
        per-dir-size (*STORM-CONF* LOGVIEWER-MAX-PER-WORKER-LOGS-SIZE-MB)
        per-dir-size (min per-dir-size (* total-size 0.5))
        dead-worker-dirs (get-dead-worker-dirs *STORM-CONF*
                                               now-secs
                                               old-log-dirs)]
    (log-debug "log cleanup: now=" now-secs
               " old log dirs " (pr-str (map #(.getName %) old-log-dirs))
               " dead worker dirs " (pr-str
                                       (map #(.getName %) dead-worker-dirs)))
    (dofor [dir dead-worker-dirs]
           (let [path (.getCanonicalPath dir)]
             (log-message "Cleaning up: Removing " path)
             (try (rmr path)
                  (cleanup-empty-topodir dir)
                  (catch Exception ex (log-error ex)))))
    (per-workerdir-cleanup (File. log-root-dir) (* per-dir-size (* 1024 1024)))
    (let [all-logs (sorted-worker-logs (File. log-root-dir))
          size (* total-size (*  1024 1024))]
      (delete-oldest-while-logs-too-large all-logs size))))

(defn start-log-cleaner! [conf log-root-dir]
  (let [interval-secs (conf LOGVIEWER-CLEANUP-INTERVAL-SECS)]
    (when interval-secs
      (log-debug "starting log cleanup thread at interval: " interval-secs)
      (schedule-recurring (mk-timer :thread-name "logviewer-cleanup"
                                    :kill-fn (fn [t]
                                               (log-error t "Error when doing logs cleanup")
                                               (exit-process! 20 "Error when doing log cleanup")))
                          0 ;; Start immediately.
                          interval-secs
                          (fn [] (cleanup-fn! log-root-dir))))))

(defn- skip-bytes
  "FileInputStream#skip may not work the first time, so ensure it successfully
  skips the given number of bytes."
  [^InputStream stream n]
  (loop [skipped 0]
    (let [skipped (+ skipped (.skip stream (- n skipped)))]
      (if (< skipped n) (recur skipped)))))

(defn logfile-matches-filter?
  [log-file-name]
  (let [regex-string (str "worker.log.*")
        regex-pattern (re-pattern regex-string)]
    (not= (re-seq regex-pattern (.toString log-file-name)) nil)))

(defn page-file
  ([path tail]
    (let [zip-file? (.endsWith path ".gz")
          flen (if zip-file? (Utils/zipFileSize (clojure.java.io/file path)) (.length (clojure.java.io/file path)))
          skip (- flen tail)]
      (page-file path skip tail)))
  ([path start length]
    (let [zip-file? (.endsWith path ".gz")
          flen (if zip-file? (Utils/zipFileSize (clojure.java.io/file path)) (.length (clojure.java.io/file path)))]
      (with-open [input (if zip-file? (GZIPInputStream. (FileInputStream. path)) (FileInputStream. path))
                  output (java.io.ByteArrayOutputStream.)]
        (if (>= start flen)
          (throw
            (InvalidRequestException. "Cannot start past the end of the file")))
        (if (> start 0) (skip-bytes input start))
        (let [buffer (make-array Byte/TYPE 1024)]
          (loop []
            (when (< (.size output) length)
              (let [size (.read input buffer 0 (min 1024 (- length (.size output))))]
                (when (pos? size)
                  (.write output buffer 0 size)
                  (recur)))))
        (.toString output))))))

(defn get-log-user-group-whitelist [fname]
  (let [wl-file (get-log-metadata-file fname)
        m (clojure-from-yaml-file wl-file)]
    (if (not-nil? m)
      (do
        (let [user-wl (.get m LOGS-USERS)
              user-wl (if user-wl user-wl [])
              group-wl (.get m LOGS-GROUPS)
              group-wl (if group-wl group-wl [])]
          [user-wl group-wl]))
        nil)))

(def igroup-mapper (AuthUtils/GetGroupMappingServiceProviderPlugin *STORM-CONF*))
(defn user-groups
  [user]
  (if (blank? user) [] (.getGroups igroup-mapper user)))

(defn authorized-log-user? [user fname conf]
  (if (or (blank? user) (blank? fname) (nil? (get-log-user-group-whitelist fname)))
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
  (let [appender (.getAppender (.getConfiguration (LogManager/getContext)) appender-name)]
    (if (and appender-name appender (instance? RollingFileAppender appender))
      (.getParent (File. (.getFileName appender)))
      (throw
       (RuntimeException. "Log viewer could not find configured appender, or the appender is not a FileAppender. Please check that the appender name configured in storm and logback agree.")))))

(defnk to-btn-link
  "Create a link that is formatted like a button"
  [url text :enabled true]
  [:a {:href (java.net.URI. url)
       :class (str "btn btn-default " (if enabled "enabled" "disabled"))} text])

(defn log-file-selection-form [log-files]
  [[:form {:action "log" :id "list-of-files"}
    (drop-down "file" log-files )
    [:input {:type "submit" :value "Switch file"}]]])

(defn pager-links [fname start length file-size]
  (let [prev-start (max 0 (- start length))
        next-start (if (> file-size 0)
                     (min (max 0 (- file-size length)) (+ start length))
                     (+ start length))]
    [[:div
      (concat
          [(to-btn-link (url "/log"
                          {:file fname
                           :start (max 0 (- start length))
                           :length length})
                          "Prev" :enabled (< prev-start start))]
          [(to-btn-link (url "/log"
                           {:file fname
                            :start 0
                            :length length}) "First")]
          [(to-btn-link (url "/log"
                           {:file fname
                            :length length})
                        "Last")]
          [(to-btn-link (url "/log"
                          {:file fname
                           :start (min (max 0 (- file-size length))
                                       (+ start length))
                           :length length})
                        "Next" :enabled (> next-start start))])]]))

(defn- download-link [fname]
  [[:p (link-to (url-format "/download/%s" fname) "Download Full File")]])

(def default-bytes-per-page 51200)

(defn log-page [fname start length grep user root-dir]
  (if (or (blank? (*STORM-CONF* UI-FILTER))
          (authorized-log-user? user fname *STORM-CONF*))
    (let [file (.getCanonicalFile (File. root-dir fname))
          path (.getCanonicalPath file)
          zip-file? (.endsWith path ".gz")
          file-length (if zip-file? (Utils/zipFileSize (clojure.java.io/file path)) (.length (clojure.java.io/file path)))
          topo-dir (.getParentFile (.getParentFile file))
          log-files (reduce clojure.set/union
                            (sorted-set)
                            (for [^File port-dir (.listFiles topo-dir)]
                              (into [] (filter #(.isFile %) (.listFiles port-dir))))) ;all types of files included
          files-str (for [file log-files]
                      (get-topo-port-workerlog file))
          reordered-files-str (conj (filter #(not= fname %) files-str) fname)]
      (if (.exists file)
        (let [length (if length
                       (min 10485760 length)
                       default-bytes-per-page)
              is-txt-file (re-find #"\.(log.*|txt|yaml)$" fname)
              log-string (escape-html
                           (if is-txt-file
                             (if start
                               (page-file path start length)
                               (page-file path length))
                             "This is a binary file and cannot display! You may download the full file."))
              start (or start (- file-length length))]
          (if grep
            (html [:pre#logContent
                   (if grep
                     (->> (.split log-string "\n")
                          (filter #(.contains % grep))
                          (string/join "\n"))
                     log-string)])
            (let [pager-data (if is-txt-file (pager-links fname start length file-length) nil)]
              (html (concat (log-file-selection-form reordered-files-str) ; list all files for this topology
                            pager-data
                            (download-link fname)
                            [[:pre#logContent log-string]]
                            pager-data)))))
        (-> (resp/response "Page not found")
            (resp/status 404))))
    (if (nil? (get-log-user-group-whitelist fname))
      (-> (resp/response "Page not found")
        (resp/status 404))
      (unauthorized-user-html user))))

(defn download-log-file [fname req resp user ^String root-dir]
  (let [file (.getCanonicalFile (File. root-dir fname))]
    (if (.exists file)
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
      (include-css "/css/bootstrap-3.3.1.min.css")
      (include-css "/css/jquery.dataTables.1.10.4.min.css")
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

(defn list-log-files
  [user topoId port log-root callback origin]
  (let [file-results
        (if (nil? topoId)
          (if (nil? port)
            (get-all-logs-for-rootdir (File. log-root))
            (reduce concat
              (for [topo-dir (.listFiles (File. log-root))]
                (reduce concat
                  (for [port-dir (.listFiles topo-dir)]
                    (if (= (str port) (.getName port-dir))
                      (into [] (.listFiles port-dir))))))))
          (if (nil? port)
            (let [topo-dir (File. (str log-root file-path-separator topoId))]
              (if (.exists topo-dir)
                (reduce concat
                  (for [port-dir (.listFiles topo-dir)]
                    (into [] (.listFiles port-dir))))
                []))
            (let [port-dir (get-worker-dir-from-root log-root topoId port)]
              (if (.exists port-dir)
                (into [] (.listFiles port-dir))
                []))))
        file-strs (sort (for [file file-results]
                          (get-topo-port-workerlog file)))]
    (json-response file-strs
      callback
      :headers {"Access-Control-Allow-Origin" origin
                "Access-Control-Allow-Credentials" "true"})))

(defroutes log-routes
  (GET "/log" [:as req & m]
    (try
      (let [servlet-request (:servlet-request req)
            log-root (:log-root req)
            user (.getUserName http-creds-handler servlet-request)
            start (if (:start m) (parse-long-from-map m :start))
            length (if (:length m) (parse-long-from-map m :length))
            file (url-decode (:file m))]
        (log-template (log-page file start length (:grep m) user log-root)
          file user))
      (catch InvalidRequestException ex
        (log-error ex)
        (ring-response-from-exception ex))))
  (GET "/download/:file" [:as {:keys [servlet-request servlet-response log-root]} file & m]
    ;; We do not use servlet-response here, but do not remove it from the
    ;; :keys list, or this rule could stop working when an authentication
    ;; filter is configured.
    (try
      (let [user (.getUserName http-creds-handler servlet-request)]
        (download-log-file file servlet-request servlet-response user log-root))
      (catch InvalidRequestException ex
        (log-error ex)
        (ring-response-from-exception ex))))
  (GET "/listLogs" [:as req & m]
    (try
      (let [servlet-request (:servlet-request req)
            user (.getUserName http-creds-handler servlet-request)]
        (list-log-files user
          (:topoId m)
          (:port m)
          (:log-root req)
          (:callback m)
          (.getHeader servlet-request "Origin")))
      (catch InvalidRequestException ex
        (log-error ex)
        (json-response (exception->json ex) (:callback m) :status 400))))
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
                            :filter-params {}}])
          https-port (int (or (conf LOGVIEWER-HTTPS-PORT) 0))
          keystore-path (conf LOGVIEWER-HTTPS-KEYSTORE-PATH)
          keystore-pass (conf LOGVIEWER-HTTPS-KEYSTORE-PASSWORD)
          keystore-type (conf LOGVIEWER-HTTPS-KEYSTORE-TYPE)
          key-password (conf LOGVIEWER-HTTPS-KEY-PASSWORD)
          truststore-path (conf LOGVIEWER-HTTPS-TRUSTSTORE-PATH)
          truststore-password (conf LOGVIEWER-HTTPS-TRUSTSTORE-PASSWORD)
          truststore-type (conf LOGVIEWER-HTTPS-TRUSTSTORE-TYPE)
          want-client-auth (conf LOGVIEWER-HTTPS-WANT-CLIENT-AUTH)
          need-client-auth (conf LOGVIEWER-HTTPS-NEED-CLIENT-AUTH)]
      (storm-run-jetty {:port (int (conf LOGVIEWER-PORT))
                        :configurator (fn [server]
                                        (config-ssl server
                                                    https-port
                                                    keystore-path
                                                    keystore-pass
                                                    keystore-type
                                                    key-password
                                                    truststore-path
                                                    truststore-password
                                                    truststore-type
                                                    want-client-auth
                                                    need-client-auth)
                                        (config-filter server middle filters-confs))}))
  (catch Exception ex
    (log-error ex))))

(defn -main []
  (let [conf (read-storm-config)
        log-root (worker-artifacts-root conf)]
    (setup-default-uncaught-exception-handler)
    (start-log-cleaner! conf log-root)
    (start-logviewer! conf log-root)))
