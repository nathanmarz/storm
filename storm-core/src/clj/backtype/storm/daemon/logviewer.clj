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
  (:import [java.util Arrays])
  (:import [java.util.zip GZIPInputStream])
  (:import [org.apache.logging.log4j LogManager])
  (:import [org.apache.logging.log4j.core Appender LoggerContext])
  (:import [org.apache.logging.log4j.core.appender RollingFileAppender])
  (:import [java.io BufferedInputStream File FileFilter FileInputStream
            InputStream InputStreamReader])
  (:import [java.nio ByteBuffer])
  (:import [org.yaml.snakeyaml Yaml]
           [org.yaml.snakeyaml.constructor SafeConstructor])
  (:import [backtype.storm.ui InvalidRequestException]
           [backtype.storm.security.auth AuthUtils])
  (:require [backtype.storm.daemon common [supervisor :as supervisor]])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.middleware.keyword-params]
            [ring.util.codec :as codec]
            [ring.util.response :as resp]
            [clojure.string :as string])
  (:require [metrics.meters :refer [defmeter mark!]])
  (:use [backtype.storm.daemon.common :only [start-metrics-reporters]])
  (:gen-class))

(def ^:dynamic *STORM-CONF* (read-storm-config))

(defmeter logviewer:num-log-page-http-requests)
(defmeter logviewer:num-daemonlog-page-http-requests)
(defmeter logviewer:num-download-log-file-http-requests)
(defmeter logviewer:num-download-log-daemon-file-http-requests)
(defmeter logviewer:num-list-logs-http-requests)

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
  "Delete the oldest files in each overloaded worker log dir"
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
       (RuntimeException. "Log viewer could not find configured appender, or the appender is not a FileAppender. Please check that the appender name configured in storm and log4j agree.")))))

(defnk to-btn-link
  "Create a link that is formatted like a button"
  [url text :enabled true]
  [:a {:href (java.net.URI. url)
       :class (str "btn btn-default " (if enabled "enabled" "disabled"))} text])

(defn search-file-form [fname]
  [[:form {:action "logviewer_search.html" :id "search-box"}
    "Search this file:"
    [:input {:type "text" :name "search"}]
    [:input {:type "hidden" :name "file" :value fname}]
    [:input {:type "submit" :value "Search"}]]])

(defn log-file-selection-form [log-files type]
  [[:form {:action type :id "list-of-files"}
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

(defn- daemon-download-link [fname]
  [[:p (link-to (url-format "/daemondownload/%s" fname) "Download Full File")]])

(defn- is-txt-file [fname]
  (re-find #"\.(log.*|txt|yaml|pid)$" fname))

(def default-bytes-per-page 51200)

(defn log-page [fname start length grep user root-dir]
  (if (or (blank? (*STORM-CONF* UI-FILTER))
          (authorized-log-user? user fname *STORM-CONF*))
    (let [file (.getCanonicalFile (File. root-dir fname))
          path (.getCanonicalPath file)
          zip-file? (.endsWith path ".gz")
          topo-dir (.getParentFile (.getParentFile file))]
      (if (and (.exists file)
               (= (.getCanonicalFile (File. root-dir))
                  (.getParentFile topo-dir)))
        (let [file-length (if zip-file? (Utils/zipFileSize (clojure.java.io/file path)) (.length (clojure.java.io/file path)))
              log-files (reduce clojure.set/union
                          (sorted-set)
                          (for [^File port-dir (.listFiles topo-dir)]
                            (into [] (filter #(.isFile %) (.listFiles port-dir))))) ;all types of files included
              files-str (for [file log-files]
                          (get-topo-port-workerlog file))
              reordered-files-str (conj (filter #(not= fname %) files-str) fname)
               length (if length
                       (min 10485760 length)
                       default-bytes-per-page)
              log-string (escape-html
                           (if (is-txt-file fname)
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
            (let [pager-data (if (is-txt-file fname) (pager-links fname start length file-length) nil)]
              (html (concat (search-file-form fname)
                            (log-file-selection-form reordered-files-str "log") ; list all files for this topology
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

(defn daemonlog-page [fname start length grep user root-dir]
  (if (or (blank? (*STORM-CONF* UI-FILTER))
        (authorized-log-user? user fname *STORM-CONF*))
    (let [file (.getCanonicalFile (File. root-dir fname))
          file-length (.length file)
          path (.getCanonicalPath file)
          zip-file? (.endsWith path ".gz")]
      (if (and (= (.getCanonicalFile (File. root-dir))
                 (.getParentFile file))
            (.exists file))
        (let [file-length (if zip-file? (Utils/zipFileSize (clojure.java.io/file path)) (.length (clojure.java.io/file path)))
              length (if length
                       (min 10485760 length)
                       default-bytes-per-page)
              log-files (into [] (filter #(.isFile %) (.listFiles (File. root-dir)))) ;all types of files included
              files-str (for [file log-files]
                          (.getName file))
              reordered-files-str (conj (filter #(not= fname %) files-str) fname)
              log-string (escape-html
                           (if (is-txt-file fname)
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
            (let [pager-data (if (is-txt-file fname) (pager-links fname start length file-length) nil)]
              (html (concat (log-file-selection-form reordered-files-str "daemonlog") ; list all daemon logs
                      pager-data
                      (daemon-download-link fname)
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

(def grep-max-search-size 1024)
(def grep-buf-size 2048)
(def grep-context-size 128)

(defn logviewer-port
  []
  (int (*STORM-CONF* LOGVIEWER-PORT)))

(defn url-to-match-centered-in-log-page
  [needle fname offset port]
  (let [host (local-hostname)
        port (logviewer-port)
        fname (clojure.string/join file-path-separator (take-last 3 (split fname (re-pattern file-path-separator))))]
    (url (str "http://" host ":" port "/log")
      {:file fname
       :start (max 0
                (- offset
                  (int (/ default-bytes-per-page 2))
                  (int (/ (alength needle) -2)))) ;; Addition
       :length default-bytes-per-page})))

(defnk mk-match-data
  [^bytes needle ^ByteBuffer haystack haystack-offset file-offset fname
   :before-bytes nil :after-bytes nil]
  (let [url (url-to-match-centered-in-log-page needle
              fname
              file-offset
              (*STORM-CONF* LOGVIEWER-PORT))
        haystack-bytes (.array haystack)
        before-string (if (>= haystack-offset grep-context-size)
                        (String. haystack-bytes
                          (- haystack-offset grep-context-size)
                          grep-context-size
                          "UTF-8")
                        (let [num-desired (max 0 (- grep-context-size
                                                   haystack-offset))
                              before-size (if before-bytes
                                            (alength before-bytes)
                                            0)
                              num-expected (min before-size num-desired)]
                          (if (pos? num-expected)
                            (str (String. before-bytes
                                   (- before-size num-expected)
                                   num-expected
                                   "UTF-8")
                              (String. haystack-bytes
                                0
                                haystack-offset
                                "UTF-8"))
                            (String. haystack-bytes
                              0
                              haystack-offset
                              "UTF-8"))))
        after-string (let [needle-size (alength needle)
                           after-offset (+ haystack-offset needle-size)
                           haystack-size (.limit haystack)]
                       (if (< (+ after-offset grep-context-size) haystack-size)
                         (String. haystack-bytes
                           after-offset
                           grep-context-size
                           "UTF-8")
                         (let [num-desired (- grep-context-size
                                             (- haystack-size after-offset))
                               after-size (if after-bytes
                                            (alength after-bytes)
                                            0)
                               num-expected (min after-size num-desired)]
                           (if (pos? num-expected)
                             (str (String. haystack-bytes
                                    after-offset
                                    (- haystack-size after-offset)
                                    "UTF-8")
                               (String. after-bytes 0 num-expected "UTF-8"))
                             (String. haystack-bytes
                               after-offset
                               (- haystack-size after-offset)
                               "UTF-8")))))]
    {"byteOffset" file-offset
     "beforeString" before-string
     "afterString" after-string
     "matchString" (String. needle "UTF-8")
     "logviewerURL" url}))

(defn- try-read-ahead!
  "Tries once to read ahead in the stream to fill the context and resets the
  stream to its position before the call."
  [^BufferedInputStream stream haystack offset file-len bytes-read]
  (let [num-expected (min (- file-len bytes-read)
                       grep-context-size)
        after-bytes (byte-array num-expected)]
    (.mark stream num-expected)
    ;; Only try reading once.
    (.read stream after-bytes 0 num-expected)
    (.reset stream)
    after-bytes))

(defn offset-of-bytes
  "Searches a given byte array for a match of a sub-array of bytes.  Returns
  the offset to the byte that matches, or -1 if no match was found."
  [^bytes buf ^bytes value init-offset]
  {:pre [(> (alength value) 0)
         (not (neg? init-offset))]}
  (loop [offset init-offset
         candidate-offset init-offset
         val-offset 0]
    (if-not (pos? (- (alength value) val-offset))
      ;; Found
      candidate-offset
      (if (>= offset (alength buf))
        ;; We ran out of buffer for the search.
        -1
        (if (not= (aget value val-offset) (aget buf offset))
          ;; The match at this candidate offset failed, so start over with the
          ;; next candidate byte from the buffer.
          (let [new-offset (inc candidate-offset)]
            (recur new-offset new-offset 0))
          ;; So far it matches.  Keep going...
          (recur (inc offset) candidate-offset (inc val-offset)))))))

(defn- buffer-substring-search!
  "As the file is read into a buffer, 1/2 the buffer's size at a time, we
  search the buffer for matches of the substring and return a list of zero or
  more matches."
  [file file-len offset-to-buf init-buf-offset stream bytes-skipped
   bytes-read ^ByteBuffer haystack ^bytes needle initial-matches num-matches
   ^bytes before-bytes]
  (loop [buf-offset init-buf-offset
         matches initial-matches]
    (let [offset (offset-of-bytes (.array haystack) needle buf-offset)]
      (if (and (< (count matches) num-matches) (not (neg? offset)))
        (let [file-offset (+ offset-to-buf offset)
              bytes-needed-after-match (- (.limit haystack)
                                         grep-context-size
                                         (alength needle))
              before-arg (if (< offset grep-context-size) before-bytes)
              after-arg (if (> offset bytes-needed-after-match)
                          (try-read-ahead! stream
                            haystack
                            offset
                            file-len
                            bytes-read))]
          (recur (+ offset (alength needle))
            (conj matches
              (mk-match-data needle
                haystack
                offset
                file-offset
                (.getCanonicalPath file)
                :before-bytes before-arg
                :after-bytes after-arg))))
        (let [before-str-to-offset (min (.limit haystack)
                                     grep-max-search-size)
              before-str-from-offset (max 0 (- before-str-to-offset
                                              grep-context-size))
              new-before-bytes (Arrays/copyOfRange (.array haystack)
                                 before-str-from-offset
                                 before-str-to-offset)
              ;; It's OK if new-byte-offset is negative.  This is normal if
              ;; we are out of bytes to read from a small file.
              new-byte-offset (if (>= (count matches) num-matches)
                                (+ (get (last matches) "byteOffset")
                                  (alength needle))
                                (+ bytes-skipped
                                  bytes-read
                                  (- grep-max-search-size)))]
          [matches new-byte-offset new-before-bytes])))))

(defn- mk-grep-response
  "This response data only includes a next byte offset if there is more of the
  file to read."
  [search-bytes offset matches next-byte-offset]
  (merge {"searchString" (String. search-bytes "UTF-8")
          "startByteOffset" offset
          "matches" matches}
    (and next-byte-offset {"nextByteOffset" next-byte-offset})))

(defn rotate-grep-buffer!
  [^ByteBuffer buf ^BufferedInputStream stream total-bytes-read file file-len]
  (let [buf-arr (.array buf)]
    ;; Copy the 2nd half of the buffer to the first half.
    (System/arraycopy buf-arr
      grep-max-search-size
      buf-arr
      0
      grep-max-search-size)
    ;; Zero-out the 2nd half to prevent accidental matches.
    (Arrays/fill buf-arr
      grep-max-search-size
      (count buf-arr)
      (byte 0))
    ;; Fill the 2nd half with new bytes from the stream.
    (let [bytes-read (.read stream
                       buf-arr
                       grep-max-search-size
                       (min file-len grep-max-search-size))]
      (.limit buf (+ grep-max-search-size bytes-read))
      (swap! total-bytes-read + bytes-read))))

(defnk substring-search
  "Searches for a substring in a log file, starting at the given offset,
  returning the given number of matches, surrounded by the given number of
  context lines.  Other information is included to be useful for progressively
  searching through a file for display in a UI. The search string must
  grep-max-search-size bytes or fewer when decoded with UTF-8."
  [file ^String search-string :num-matches 10 :start-byte-offset 0]
  {:pre [(not (empty? search-string))
         (<= (count (.getBytes search-string "UTF-8")) grep-max-search-size)]}
  (let [zip-file? (.endsWith (.getName file) ".gz")
        f-input-steam (FileInputStream. file)
        gzipped-input-stream (if zip-file?
                               (GZIPInputStream. f-input-steam)
                               f-input-steam)
        stream ^BufferedInputStream (BufferedInputStream.
                                      gzipped-input-stream)
        file-len (if zip-file? (Utils/zipFileSize file) (.length file))
        buf ^ByteBuffer (ByteBuffer/allocate grep-buf-size)
        buf-arr ^bytes (.array buf)
        string nil
        total-bytes-read (atom 0)
        matches []
        search-bytes ^bytes (.getBytes search-string "UTF-8")
        num-matches (or num-matches 10)
        start-byte-offset (or start-byte-offset 0)]
    ;; Start at the part of the log file we are interested in.
    ;; Allow searching when start-byte-offset == file-len so it doesn't blow up on 0-length files
    (if (> start-byte-offset file-len)
      (throw
        (InvalidRequestException. "Cannot search past the end of the file")))
    (when (> start-byte-offset 0)
      (skip-bytes stream start-byte-offset))
    (java.util.Arrays/fill buf-arr (byte 0))
    (let [bytes-read (.read stream buf-arr 0 (min file-len grep-buf-size))]
      (.limit buf bytes-read)
      (swap! total-bytes-read + bytes-read))
    (loop [initial-matches []
           init-buf-offset 0
           byte-offset start-byte-offset
           before-bytes nil]
      (let [[matches new-byte-offset new-before-bytes]
            (buffer-substring-search! file
              file-len
              byte-offset
              init-buf-offset
              stream
              start-byte-offset
              @total-bytes-read
              buf
              search-bytes
              initial-matches
              num-matches
              before-bytes)]
        (if (and (< (count matches) num-matches)
              (< (+ @total-bytes-read start-byte-offset) file-len))
          (let [;; The start index is positioned to find any possible
                ;; occurrence search string that did not quite fit in the
                ;; buffer on the previous read.
                new-buf-offset (- (min (.limit ^ByteBuffer buf)
                                    grep-max-search-size)
                                 (alength search-bytes))]
            (rotate-grep-buffer! buf stream total-bytes-read file file-len)
            (when (< @total-bytes-read 0)
              (throw (InvalidRequestException. "Cannot search past the end of the file")))
            (recur matches
              new-buf-offset
              new-byte-offset
              new-before-bytes))
          (mk-grep-response search-bytes
            start-byte-offset
            matches
            (if-not (and (< (count matches) num-matches)
                      (>= @total-bytes-read file-len))
              (let [next-byte-offset (+ (get (last matches)
                                          "byteOffset")
                                       (alength search-bytes))]
                (if (> file-len next-byte-offset)
                  next-byte-offset)))))))))

(defn- try-parse-int-param
  [nam value]
  (try
    (Integer/parseInt value)
    (catch java.lang.NumberFormatException e
      (->
        (str "Could not parse " nam " to an integer")
        (InvalidRequestException. e)
        throw))))

(defn search-log-file
  [fname user ^String root-dir search num-matches offset callback origin]
  (let [file (.getCanonicalFile (File. root-dir fname))]
    (if (.exists file)
      (if (or (blank? (*STORM-CONF* UI-FILTER))
            (authorized-log-user? user fname *STORM-CONF*))
        (let [num-matches-int (if num-matches
                                (try-parse-int-param "num-matches"
                                  num-matches))
              offset-int (if offset
                           (try-parse-int-param "start-byte-offset" offset))]
          (try
            (if (and (not (empty? search))
                  <= (count (.getBytes search "UTF-8")) grep-max-search-size)
              (json-response
                (substring-search file
                  search
                  :num-matches num-matches-int
                  :start-byte-offset offset-int)
                callback
                :headers {"Access-Control-Allow-Origin" origin
                          "Access-Control-Allow-Credentials" "true"})
              (throw
                (InvalidRequestException.
                  (str "Search substring must be between 1 and 1024 UTF-8 "
                    "bytes in size (inclusive)"))))
            (catch Exception ex
              (json-response (exception->json ex) callback :status 500))))
        (json-response (unauthorized-user-json user) callback :status 401))
      (json-response {"error" "Not Found"
                      "errorMessage" "The file was not found on this node."}
        callback
        :status 404))))

(defn find-n-matches [logs n file-offset offset search]
  (let [logs (drop file-offset logs)
        wrap-matches-fn (fn [matches]
                          {"fileOffset" file-offset
                           "searchString" search
                           "matches" matches})]
    (loop [matches []
           logs logs
           offset offset
           file-offset file-offset
           match-count 0]
      (if (empty? logs)
        (wrap-matches-fn matches)
        (let [these-matches (try
                              (log-debug "Looking through " (first logs))
                              (substring-search (first logs)
                                search
                                :num-matches (- n match-count)
                                :start-byte-offset offset)
                              (catch InvalidRequestException e
                                (log-error e "Can't search past end of file.")
                                {}))
              file-name (get-topo-port-workerlog (first logs))
              new-matches (conj matches
                            (merge these-matches
                              { "fileName" file-name
                                "port" (first (take-last 2 (split (.getCanonicalPath (first logs)) (re-pattern file-path-separator))))}))
              new-count (+ match-count (count (these-matches "matches")))]
          (if (empty? these-matches)
            (recur matches (rest logs) 0 (+ file-offset 1) match-count)
            (if (>= new-count n)
              (wrap-matches-fn new-matches)
              (recur new-matches (rest logs) 0 (+ file-offset 1) new-count))))))))

(defn logs-for-port
  "Get the filtered, authorized, sorted log files for a port."
  [user port-dir]
  (let [filter-authorized-fn (fn [user logs]
                               (filter #(or
                                          (blank? (*STORM-CONF* UI-FILTER))
                                          (authorized-log-user? user (get-topo-port-workerlog %) *STORM-CONF*)) logs))]
    (sort #(compare (.lastModified %2) (.lastModified %1))
      (filter-authorized-fn
        user
        (filter #(re-find worker-log-filename-pattern (.getName %)) (.listFiles port-dir))))))

(defn deep-search-logs-for-topology
  [topology-id user ^String root-dir search num-matches port file-offset offset search-archived? callback origin]
  (json-response
    (if (or (not search) (not (.exists (File. (str root-dir file-path-separator topology-id)))))
      []
      (let [file-offset (if file-offset (Integer/parseInt file-offset) 0)
            offset (if offset (Integer/parseInt offset) 0)
            num-matches (or (Integer/parseInt num-matches) 1)
            port-dirs (vec (.listFiles (File. (str root-dir file-path-separator topology-id))))
            logs-for-port-fn (partial logs-for-port user)]
        (if (or (not port) (= "*" port))
          ;; Check for all ports
          (let [filtered-logs (filter (comp not empty?) (map logs-for-port-fn port-dirs))]
            (if search-archived?
              (map #(find-n-matches % num-matches 0 0 search)
                filtered-logs)
              (map #(find-n-matches % num-matches 0 0 search)
                (map (comp vector first) filtered-logs))))
          ;; Check just the one port
          (if (not (contains? (into #{} (map str (*STORM-CONF* SUPERVISOR-SLOTS-PORTS))) port))
            []
            (let [port-dir (File. (str root-dir file-path-separator topology-id file-path-separator port))]
              (if (or (not (.exists port-dir)) (empty? (logs-for-port user port-dir)))
                []
                (let [filtered-logs (logs-for-port user port-dir)]
                  (if search-archived?
                    (find-n-matches filtered-logs num-matches file-offset offset search)
                    (find-n-matches [(first filtered-logs)] num-matches 0 offset search)))))))))
    callback
    :headers {"Access-Control-Allow-Origin" origin
              "Access-Control-Allow-Credentials" "true"}))

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

(defn get-profiler-dump-files
  [dir]
  (filter (comp not nil?)
        (for [f (.listFiles dir)]
          (let [name (.getName f)]
            (if (or
                  (.endsWith name ".txt")
                  (.endsWith name ".jfr")
                  (.endsWith name ".bin"))
              (.getName f))))))

(defroutes log-routes
  (GET "/log" [:as req & m]
    (try
      (mark! logviewer:num-log-page-http-requests)
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
  (GET "/dumps/:topo-id/:host-port/:filename"
       [:as {:keys [servlet-request servlet-response log-root]} topo-id host-port filename &m]
     (let [user (.getUserName http-creds-handler servlet-request)
           port (second (split host-port #":"))
           dir (File. (str log-root
                           file-path-separator
                           topo-id
                           file-path-separator
                           port))
           file (File. (str log-root
                            file-path-separator
                            topo-id
                            file-path-separator
                            port
                            file-path-separator
                            filename))]
       (if (and (.exists dir) (.exists file))
         (if (or (blank? (*STORM-CONF* UI-FILTER))
               (authorized-log-user? user 
                                     (str topo-id file-path-separator port file-path-separator "worker.log")
                                     *STORM-CONF*))
           (-> (resp/response file)
               (resp/content-type "application/octet-stream"))
           (unauthorized-user-html user))
         (-> (resp/response "Page not found")
           (resp/status 404)))))
  (GET "/dumps/:topo-id/:host-port"
       [:as {:keys [servlet-request servlet-response log-root]} topo-id host-port &m]
     (let [user (.getUserName http-creds-handler servlet-request)
           port (second (split host-port #":"))
           dir (File. (str log-root
                           file-path-separator
                           topo-id
                           file-path-separator
                           port))]
       (if (.exists dir)
         (if (or (blank? (*STORM-CONF* UI-FILTER))
               (authorized-log-user? user 
                                     (str topo-id file-path-separator port file-path-separator "worker.log")
                                     *STORM-CONF*))
           (html4
             [:head
              [:title "File Dumps - Storm Log Viewer"]
              (include-css "/css/bootstrap-3.3.1.min.css")
              (include-css "/css/jquery.dataTables.1.10.4.min.css")
              (include-css "/css/style.css")]
             [:body
              [:ul
               (for [file (get-profiler-dump-files dir)]
                 [:li
                  [:a {:href (str "/dumps/" topo-id "/" host-port "/" file)} file ]])]])
           (unauthorized-user-html user))
         (-> (resp/response "Page not found")
           (resp/status 404)))))
  (GET "/daemonlog" [:as req & m]
    (try
      (mark! logviewer:num-daemonlog-page-http-requests)
      (let [servlet-request (:servlet-request req)
            daemonlog-root (:daemonlog-root req)
            user (.getUserName http-creds-handler servlet-request)
            start (if (:start m) (parse-long-from-map m :start))
            length (if (:length m) (parse-long-from-map m :length))
            file (url-decode (:file m))]
        (log-template (daemonlog-page file start length (:grep m) user daemonlog-root)
          file user))
      (catch InvalidRequestException ex
        (log-error ex)
        (ring-response-from-exception ex))))
  (GET "/download/:file" [:as {:keys [servlet-request servlet-response log-root]} file & m]
    (try
      (mark! logviewer:num-download-log-file-http-requests)
      (let [user (.getUserName http-creds-handler servlet-request)]
        (download-log-file file servlet-request servlet-response user log-root))
      (catch InvalidRequestException ex
        (log-error ex)
        (ring-response-from-exception ex))))
  (GET "/daemondownload/:file" [:as {:keys [servlet-request servlet-response daemonlog-root]} file & m]
    (try
      (mark! logviewer:num-download-log-daemon-file-http-requests)
      (let [user (.getUserName http-creds-handler servlet-request)]
        (download-log-file file servlet-request servlet-response user daemonlog-root))
      (catch InvalidRequestException ex
        (log-error ex)
        (ring-response-from-exception ex))))
  (GET "/search/:file" [:as {:keys [servlet-request servlet-response log-root]} file & m]
    ;; We do not use servlet-response here, but do not remove it from the
    ;; :keys list, or this rule could stop working when an authentication
    ;; filter is configured.
    (try
      (let [user (.getUserName http-creds-handler servlet-request)]
        (search-log-file (url-decode file)
          user
          log-root
          (:search-string m)
          (:num-matches m)
          (:start-byte-offset m)
          (:callback m)
          (.getHeader servlet-request "Origin")))
      (catch InvalidRequestException ex
        (log-error ex)
        (json-response (exception->json ex) (:callback m) :status 400))))
  (GET "/deepSearch/:topo-id" [:as {:keys [servlet-request servlet-response log-root]} topo-id & m]
    ;; We do not use servlet-response here, but do not remove it from the
    ;; :keys list, or this rule could stop working when an authentication
    ;; filter is configured.
    (try
      (let [user (.getUserName http-creds-handler servlet-request)]
        (deep-search-logs-for-topology topo-id
          user
          log-root
          (:search-string m)
          (:num-matches m)
          (:port m)
          (:start-file-offset m)
          (:start-byte-offset m)
          (:search-archived m)
          (:callback m)
          (.getHeader servlet-request "Origin")))
      (catch InvalidRequestException ex
        (log-error ex)
        (json-response (exception->json ex) (:callback m) :status 400))))
  (GET "/searchLogs" [:as req & m]
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
  (GET "/listLogs" [:as req & m]
    (try
      (mark! logviewer:num-list-logs-http-requests)
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
  [app log-root daemonlog-root]
  (fn [req]
    (app (assoc req :log-root log-root :daemonlog-root daemonlog-root))))

(defn start-logviewer! [conf log-root-dir daemonlog-root-dir]
  (try
    (let [header-buffer-size (int (.get conf UI-HEADER-BUFFER-BYTES))
          filter-class (conf UI-FILTER)
          filter-params (conf UI-FILTER-PARAMS)
          logapp (handler/api (-> log-routes
                                requests-middleware))  ;; query params as map
          middle (conf-middleware logapp log-root-dir daemonlog-root-dir)
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
        log-root (worker-artifacts-root conf)
        daemonlog-root (log-root-dir (conf LOGVIEWER-APPENDER-NAME))]
    (setup-default-uncaught-exception-handler)
    (start-log-cleaner! conf log-root)
    (start-logviewer! conf log-root daemonlog-root)
    (start-metrics-reporters)))
