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

(ns backtype.storm.util
  (:import [java.net InetAddress])
  (:import [java.util Map Map$Entry List ArrayList Collection Iterator HashMap])
  (:import [java.io FileReader FileNotFoundException])
  (:import [backtype.storm Config])
  (:import [backtype.storm.utils Time Container ClojureTimerTask Utils
            MutableObject MutableInt])
  (:import [java.util UUID Random ArrayList List Collections])
  (:import [java.util.zip ZipFile])
  (:import [java.util.concurrent.locks ReentrantReadWriteLock])
  (:import [java.util.concurrent Semaphore])
  (:import [java.io File FileOutputStream StringWriter PrintWriter IOException])
  (:import [java.lang.management ManagementFactory])
  (:import [org.apache.commons.exec DefaultExecutor CommandLine])
  (:import [org.apache.commons.io FileUtils])
  (:import [org.apache.commons.exec ExecuteException])
  (:import [org.json.simple JSONValue])
  (:require [clojure [string :as str]])
  (:import [clojure.lang RT])
  (:require [clojure [set :as set]])
  (:require [clojure.java.io :as io])
  (:use [clojure walk])
  (:use [backtype.storm log]))

(defn wrap-in-runtime
  "Wraps an exception in a RuntimeException if needed"
  [^Exception e]
  (if (instance? RuntimeException e)
    e
    (RuntimeException. e)))

(def on-windows?
  (= "Windows_NT" (System/getenv "OS")))

(def file-path-separator
  (System/getProperty "file.separator"))

(def class-path-separator
  (System/getProperty "path.separator"))

(defmacro defalias
  "Defines an alias for a var: a new var with the same root binding (if
  any) and similar metadata. The metadata of the alias is its initial
  metadata (as provided by def) merged into the metadata of the original."
  ([name orig]
   `(do
      (alter-meta!
        (if (.hasRoot (var ~orig))
          (def ~name (.getRawRoot (var ~orig)))
          (def ~name))
        ;; When copying metadata, disregard {:macro false}.
        ;; Workaround for http://www.assembla.com/spaces/clojure/tickets/273
        #(conj (dissoc % :macro)
               (apply dissoc (meta (var ~orig)) (remove #{:macro} (keys %)))))
      (var ~name)))
  ([name orig doc]
   (list `defalias (with-meta name (assoc (meta name) :doc doc)) orig)))

;; name-with-attributes by Konrad Hinsen:
(defn name-with-attributes
  "To be used in macro definitions.
  Handles optional docstrings and attribute maps for a name to be defined
  in a list of macro arguments. If the first macro argument is a string,
  it is added as a docstring to name and removed from the macro argument
  list. If afterwards the first macro argument is a map, its entries are
  added to the name's metadata map and the map is removed from the
  macro argument list. The return value is a vector containing the name
  with its extended metadata map and the list of unprocessed macro
  arguments."
  [name macro-args]
  (let [[docstring macro-args] (if (string? (first macro-args))
                                 [(first macro-args) (next macro-args)]
                                 [nil macro-args])
        [attr macro-args] (if (map? (first macro-args))
                            [(first macro-args) (next macro-args)]
                            [{} macro-args])
        attr (if docstring
               (assoc attr :doc docstring)
               attr)
        attr (if (meta name)
               (conj (meta name) attr)
               attr)]
    [(with-meta name attr) macro-args]))

(defmacro defnk
  "Define a function accepting keyword arguments. Symbols up to the first
  keyword in the parameter list are taken as positional arguments.  Then
  an alternating sequence of keywords and defaults values is expected. The
  values of the keyword arguments are available in the function body by
  virtue of the symbol corresponding to the keyword (cf. :keys destructuring).
  defnk accepts an optional docstring as well as an optional metadata map."
  [fn-name & fn-tail]
  (let [[fn-name [args & body]] (name-with-attributes fn-name fn-tail)
        [pos kw-vals] (split-with symbol? args)
        syms (map #(-> % name symbol) (take-nth 2 kw-vals))
        values (take-nth 2 (rest kw-vals))
        sym-vals (apply hash-map (interleave syms values))
        de-map {:keys (vec syms) :or sym-vals}]
    `(defn ~fn-name
       [~@pos & options#]
       (let [~de-map (apply hash-map options#)]
         ~@body))))

(defn find-first
  "Returns the first item of coll for which (pred item) returns logical true.
  Consumes sequences up to the first match, will consume the entire sequence
  and return nil if no match is found."
  [pred coll]
  (first (filter pred coll)))

(defn dissoc-in
  "Dissociates an entry from a nested associative structure returning a new
  nested structure. keys is a sequence of keys. Any empty maps that result
  will not be present in the new structure."
  [m [k & ks :as keys]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))

(defn indexed
  "Returns a lazy sequence of [index, item] pairs, where items come
  from 's' and indexes count up from zero.

  (indexed '(a b c d))  =>  ([0 a] [1 b] [2 c] [3 d])"
  [s]
  (map vector (iterate inc 0) s))

(defn positions
  "Returns a lazy sequence containing the positions at which pred
  is true for items in coll."
  [pred coll]
  (for [[idx elt] (indexed coll) :when (pred elt)] idx))

(defn exception-cause?
  [klass ^Throwable t]
  (->> (iterate #(.getCause ^Throwable %) t)
       (take-while identity)
       (some (partial instance? klass))
       boolean))

(defmacro thrown-cause?
  [klass & body]
  `(try
     ~@body
     false
     (catch Throwable t#
       (exception-cause? ~klass t#))))

(defmacro thrown-cause-with-msg?
  [klass re & body]
  `(try
     ~@body
     false
     (catch Throwable t#
       (and (re-matches ~re (.getMessage t#))
            (exception-cause? ~klass t#)))))

(defmacro forcat
  [[args aseq] & body]
  `(mapcat (fn [~args]
             ~@body)
           ~aseq))

(defmacro try-cause
  [& body]
  (let [checker (fn [form]
                  (or (not (sequential? form))
                      (not= 'catch (first form))))
        [code guards] (split-with checker body)
        error-local (gensym "t")
        guards (forcat [[_ klass local & guard-body] guards]
                       `((exception-cause? ~klass ~error-local)
                         (let [~local ~error-local]
                           ~@guard-body
                           )))]
    `(try ~@code
       (catch Throwable ~error-local
         (cond ~@guards
               true (throw ~error-local)
               )))))

(defn local-hostname
  []
  (.getCanonicalHostName (InetAddress/getLocalHost)))

(def memoized-local-hostname (memoize local-hostname))

(letfn [(try-port [port]
                  (with-open [socket (java.net.ServerSocket. port)]
                    (.getLocalPort socket)))]
  (defn available-port
    ([] (try-port 0))
    ([preferred]
     (try
       (try-port preferred)
       (catch java.io.IOException e
         (available-port))))))

(defn uuid []
  (str (UUID/randomUUID)))

(defn current-time-secs
  []
  (Time/currentTimeSecs))

(defn current-time-millis
  []
  (Time/currentTimeMillis))

(defn secs-to-millis-long
  [secs]
  (long (* (long 1000) secs)))

(defn clojurify-structure
  [s]
  (prewalk (fn [x]
             (cond (instance? Map x) (into {} x)
                   (instance? List x) (vec x)
                   true x))
           s))

(defmacro with-file-lock
  [path & body]
  `(let [f# (File. ~path)
         _# (.createNewFile f#)
         rf# (RandomAccessFile. f# "rw")
         lock# (.. rf# (getChannel) (lock))]
     (try
       ~@body
       (finally
         (.release lock#)
         (.close rf#)))))

(defn tokenize-path
  [^String path]
  (let [toks (.split path "/")]
    (vec (filter (complement empty?) toks))))

(defn assoc-conj
  [m k v]
  (merge-with concat m {k [v]}))

;; returns [ones in first set not in second, ones in second set not in first]
(defn set-delta
  [old curr]
  (let [s1 (set old)
        s2 (set curr)]
    [(set/difference s1 s2) (set/difference s2 s1)]))

(defn parent-path
  [path]
  (let [toks (tokenize-path path)]
    (str "/" (str/join "/" (butlast toks)))))

(defn toks->path
  [toks]
  (str "/" (str/join "/" toks)))

(defn normalize-path
  [^String path]
  (toks->path (tokenize-path path)))

(defn map-val
  [afn amap]
  (into {}
        (for [[k v] amap]
          [k (afn v)])))

(defn filter-val
  [afn amap]
  (into {} (filter (fn [[k v]] (afn v)) amap)))

(defn filter-key
  [afn amap]
  (into {} (filter (fn [[k v]] (afn k)) amap)))

(defn map-key
  [afn amap]
  (into {} (for [[k v] amap] [(afn k) v])))

(defn separate
  [pred aseq]
  [(filter pred aseq) (filter (complement pred) aseq)])

(defn full-path
  [parent name]
  (let [toks (tokenize-path parent)]
    (toks->path (conj toks name))))

(def not-nil? (complement nil?))

(defn barr
  [& vals]
  (byte-array (map byte vals)))

(defn exit-process!
  [val & msg]
  (log-message "Halting process: " msg)
  (.exit (Runtime/getRuntime) val))

(defn sum
  [vals]
  (reduce + vals))

(defn repeat-seq
  ([aseq]
   (apply concat (repeat aseq)))
  ([amt aseq]
   (apply concat (repeat amt aseq))))

(defn div
  "Perform floating point division on the arguments."
  [f & rest]
  (apply / (double f) rest))

(defn defaulted
  [val default]
  (if val val default))

(defn mk-counter
  ([] (mk-counter 1))
  ([start-val]
   (let [val (atom (dec start-val))]
     (fn [] (swap! val inc)))))

(defmacro for-times [times & body]
  `(for [i# (range ~times)]
     ~@body))

(defmacro dofor [& body]
  `(doall (for ~@body)))

(defn reverse-map
  "{:a 1 :b 1 :c 2} -> {1 [:a :b] 2 :c}"
  [amap]
  (reduce (fn [m [k v]]
            (let [existing (get m v [])]
              (assoc m v (conj existing k))))
          {} amap))

(defmacro print-vars [& vars]
  (let [prints (for [v vars] `(println ~(str v) ~v))]
    `(do ~@prints)))

(defn process-pid
  "Gets the pid of this JVM. Hacky because Java doesn't provide a real way to do this."
  []
  (let [name (.getName (ManagementFactory/getRuntimeMXBean))
        split (.split name "@")]
    (when-not (= 2 (count split))
      (throw (RuntimeException. (str "Got unexpected process name: " name))))
    (first split)))

(defn exec-command! [command]
  (let [[comm-str & args] (seq (.split command " "))
        command (CommandLine. comm-str)]
    (doseq [a args]
      (.addArgument command a))
    (.execute (DefaultExecutor.) command)))

(defn extract-dir-from-jar [jarpath dir destdir]
  (try-cause
    (with-open [jarpath (ZipFile. jarpath)]
      (let [entries (enumeration-seq (.entries jarpath))]
        (doseq [file (filter (fn [entry](and (not (.isDirectory entry)) (.startsWith (.getName entry) dir))) entries)]
          (.mkdirs (.getParentFile (File. destdir (.getName file))))
          (with-open [out (FileOutputStream. (File. destdir (.getName file)))]
            (io/copy (.getInputStream jarpath file) out)))))
    (catch IOException e
      (log-message "Could not extract " dir " from " jarpath))))

(defn sleep-secs [secs]
  (when (pos? secs)
    (Time/sleep (* (long secs) 1000))))

(defn sleep-until-secs [target-secs]
  (Time/sleepUntil (* (long target-secs) 1000)))

(def ^:const sig-kill 9)

(def ^:const sig-term 15)

(defn send-signal-to-process
  [pid signum]
  (try-cause
    (exec-command! (str (if on-windows?
                          (if (== signum sig-kill) "taskkill /f /pid " "taskkill /pid ")
                          (str "kill -" signum " "))
                     pid))
    (catch ExecuteException e
      (log-message "Error when trying to kill " pid ". Process is probably already dead."))))

(defn force-kill-process
  [pid]
  (send-signal-to-process pid sig-kill))

(defn kill-process-with-sig-term
  [pid]
  (send-signal-to-process pid sig-term))

(defn add-shutdown-hook-with-force-kill-in-1-sec
  "adds the user supplied function as a shutdown hook for cleanup.
   Also adds a function that sleeps for a second and then sends kill -9 to process to avoid any zombie process in case
   cleanup function hangs."
  [func]
  (.addShutdownHook (Runtime/getRuntime) (Thread. #(func)))
  (.addShutdownHook (Runtime/getRuntime) (Thread. #((sleep-secs 1)
                                                    (.halt (Runtime/getRuntime) 20)))))

(defnk launch-process [command :environment {}]
  (let [builder (ProcessBuilder. command)
        process-env (.environment builder)]
    (doseq [[k v] environment]
      (.put process-env k v))
    (.start builder)))

(defprotocol SmartThread
  (start [this])
  (join [this])
  (interrupt [this])
  (sleeping? [this]))

;; afn returns amount of time to sleep
(defnk async-loop [afn
                   :daemon false
                   :kill-fn (fn [error] (exit-process! 1 "Async loop died!"))
                   :priority Thread/NORM_PRIORITY
                   :factory? false
                   :start true
                   :thread-name nil]
  (let [thread (Thread.
                 (fn []
                   (try-cause
                     (let [afn (if factory? (afn) afn)]
                       (loop []
                         (let [sleep-time (afn)]
                           (when-not (nil? sleep-time)
                             (sleep-secs sleep-time)
                             (recur))
                           )))
                     (catch InterruptedException e
                       (log-message "Async loop interrupted!")
                       )
                     (catch Throwable t
                       (log-error t "Async loop died!")
                       (kill-fn t)))))]
    (.setDaemon thread daemon)
    (.setPriority thread priority)
    (when thread-name
      (.setName thread (str (.getName thread) "-" thread-name)))
    (when start
      (.start thread))
    ;; should return object that supports stop, interrupt, join, and waiting?
    (reify SmartThread
      (start
        [this]
        (.start thread))
      (join
        [this]
        (.join thread))
      (interrupt
        [this]
        (.interrupt thread))
      (sleeping?
        [this]
        (Time/isThreadWaiting thread)))))

(defn exists-file?
  [path]
  (.exists (File. path)))

(defn rmr
  [path]
  (log-debug "Rmr path " path)
  (when (exists-file? path)
    (try
      (FileUtils/forceDelete (File. path))
      (catch FileNotFoundException e))))

(defn rmpath
  "Removes file or directory at the path. Not recursive. Throws exception on failure"
  [path]
  (log-debug "Removing path " path)
  (when (exists-file? path)
    (let [deleted? (.delete (File. path))]
      (when-not deleted?
        (throw (RuntimeException. (str "Failed to delete " path)))))))

(defn local-mkdirs
  [path]
  (log-debug "Making dirs at " path)
  (FileUtils/forceMkdir (File. path)))

(defn touch
  [path]
  (log-debug "Touching file at " path)
  (let [success? (do (if on-windows? (.mkdirs (.getParentFile (File. path))))
                   (.createNewFile (File. path)))]
    (when-not success?
      (throw (RuntimeException. (str "Failed to touch " path))))))

(defn read-dir-contents
  [dir]
  (if (exists-file? dir)
    (let [content-files (.listFiles (File. dir))]
      (map #(.getName ^File %) content-files))
    []))

(defn compact
  [aseq]
  (filter (complement nil?) aseq))

(defn current-classpath
  []
  (System/getProperty "java.class.path"))

(defn add-to-classpath
  [classpath paths]
  (if (empty? paths)
    classpath
    (str/join class-path-separator (cons classpath paths))))

(defn ^ReentrantReadWriteLock mk-rw-lock
  []
  (ReentrantReadWriteLock.))

(defmacro read-locked
  [rw-lock & body]
  (let [lock (with-meta rw-lock {:tag `ReentrantReadWriteLock})]
    `(let [rlock# (.readLock ~lock)]
       (try (.lock rlock#)
         ~@body
         (finally (.unlock rlock#))))))

(defmacro write-locked
  [rw-lock & body]
  (let [lock (with-meta rw-lock {:tag `ReentrantReadWriteLock})]
    `(let [wlock# (.writeLock ~lock)]
       (try (.lock wlock#)
         ~@body
         (finally (.unlock wlock#))))))

(defn wait-for-condition
  [apredicate]
  (while (not (apredicate))
    (Time/sleep 100)))

(defn some?
  [pred aseq]
  ((complement nil?) (some pred aseq)))

(defn time-delta
  [time-secs]
  (- (current-time-secs) time-secs))

(defn time-delta-ms
  [time-ms]
  (- (System/currentTimeMillis) (long time-ms)))

(defn parse-int
  [str]
  (Integer/valueOf str))

(defn integer-divided
  [sum num-pieces]
  (clojurify-structure (Utils/integerDivided sum num-pieces)))

(defn collectify
  [obj]
  (if (or (sequential? obj) (instance? Collection obj))
    obj
    [obj]))

(defn to-json
  [obj]
  (JSONValue/toJSONString obj))

(defn from-json
  [^String str]
  (if str
    (clojurify-structure
      (JSONValue/parse str))
    nil))

(defmacro letlocals
  [& body]
  (let [[tobind lexpr] (split-at (dec (count body)) body)
        binded (vec (mapcat (fn [e]
                              (if (and (list? e) (= 'bind (first e)))
                                [(second e) (last e)]
                                ['_ e]
                                ))
                            tobind))]
    `(let ~binded
       ~(first lexpr))))

(defn remove-first
  [pred aseq]
  (let [[b e] (split-with (complement pred) aseq)]
    (when (empty? e)
      (throw (IllegalArgumentException. "Nothing to remove")))
    (concat b (rest e))))

(defn assoc-non-nil
  [m k v]
  (if v (assoc m k v) m))

(defn multi-set
  "Returns a map of elem to count"
  [aseq]
  (apply merge-with +
         (map #(hash-map % 1) aseq)))

(defn set-var-root*
  [avar val]
  (alter-var-root avar (fn [avar] val)))

(defmacro set-var-root
  [var-sym val]
  `(set-var-root* (var ~var-sym) ~val))

(defmacro with-var-roots
  [bindings & body]
  (let [settings (partition 2 bindings)
        tmpvars (repeatedly (count settings) (partial gensym "old"))
        vars (map first settings)
        savevals (vec (mapcat (fn [t v] [t v]) tmpvars vars))
        setters (for [[v s] settings] `(set-var-root ~v ~s))
        restorers (map (fn [v s] `(set-var-root ~v ~s)) vars tmpvars)]
    `(let ~savevals
       ~@setters
       (try
         ~@body
         (finally
           ~@restorers)))))

(defn map-diff
  "Returns mappings in m2 that aren't in m1"
  [m1 m2]
  (into {} (filter (fn [[k v]] (not= v (m1 k))) m2)))

(defn select-keys-pred
  [pred amap]
  (into {} (filter (fn [[k v]] (pred k)) amap)))

(defn rotating-random-range
  [choices]
  (let [rand (Random.)
        choices (ArrayList. choices)]
    (Collections/shuffle choices rand)
    [(MutableInt. -1) choices rand]))

(defn acquire-random-range-id
  [[^MutableInt curr ^List state ^Random rand]]
  (when (>= (.increment curr) (.size state))
    (.set curr 0)
    (Collections/shuffle state rand))
  (.get state (.get curr)))

; this can be rewritten to be tail recursive
(defn interleave-all
  [& colls]
  (if (empty? colls)
    []
    (let [colls (filter (complement empty?) colls)
          my-elems (map first colls)
          rest-elems (apply interleave-all (map rest colls))]
      (concat my-elems rest-elems))))

(defn update
  [m k afn]
  (assoc m k (afn (get m k))))

(defn any-intersection
  [& sets]
  (let [elem->count (multi-set (apply concat sets))]
    (-> (filter-val #(> % 1) elem->count)
        keys)))

(defn between?
  "val >= lower and val <= upper"
  [val lower upper]
  (and (>= val lower)
       (<= val upper)))

(defmacro benchmark
  [& body]
  `(let [l# (doall (range 1000000))]
     (time
       (doseq [i# l#]
         ~@body))))

(defn rand-sampler
  [freq]
  (let [r (java.util.Random.)]
    (fn [] (= 0 (.nextInt r freq)))))

(defn even-sampler
  [freq]
  (let [freq (int freq)
        start (int 0)
        r (java.util.Random.)
        curr (MutableInt. -1)
        target (MutableInt. (.nextInt r freq))]
    (with-meta
      (fn []
        (let [i (.increment curr)]
          (when (>= i freq)
            (.set curr start)
            (.set target (.nextInt r freq))))
        (= (.get curr) (.get target)))
      {:rate freq})))

(defn sampler-rate
  [sampler]
  (:rate (meta sampler)))

(defn class-selector
  [obj & args]
  (class obj))

(defn uptime-computer []
  (let [start-time (current-time-secs)]
    (fn [] (time-delta start-time))))

(defn stringify-error [error]
  (let [result (StringWriter.)
        printer (PrintWriter. result)]
    (.printStackTrace error printer)
    (.toString result)))

(defn nil-to-zero
  [v]
  (or v 0))

(defn bit-xor-vals
  [vals]
  (reduce bit-xor 0 vals))

(defmacro with-error-reaction
  [afn & body]
  `(try ~@body
     (catch Throwable t# (~afn t#))))

(defn container
  []
  (Container.))

(defn container-set! [^Container container obj]
  (set! (. container object) obj)
  container)

(defn container-get [^Container container]
  (. container object))

(defn to-millis [secs]
  (* 1000 (long secs)))

(defn throw-runtime [& strs]
  (throw (RuntimeException. (apply str strs))))

(defn redirect-stdio-to-slf4j!
  []
  ;; set-var-root doesn't work with *out* and *err*, so digging much deeper here
  ;; Unfortunately, this code seems to work at the REPL but not when spawned as worker processes
  ;; it might have something to do with being a child process
  ;; (set! (. (.getThreadBinding RT/OUT) val)
  ;;       (java.io.OutputStreamWriter.
  ;;         (log-stream :info "STDIO")))
  ;; (set! (. (.getThreadBinding RT/ERR) val)
  ;;       (PrintWriter.
  ;;         (java.io.OutputStreamWriter.
  ;;           (log-stream :error "STDIO"))
  ;;         true))
  (log-capture! "STDIO"))

(defn spy
  [prefix val]
  (log-message prefix ": " val)
  val)

(defn zip-contains-dir?
  [zipfile target]
  (let [entries (->> zipfile (ZipFile.) .entries enumeration-seq (map (memfn getName)))]
    (some? #(.startsWith % (str target "/")) entries)))

(defn url-encode
  [s]
  (java.net.URLEncoder/encode s "UTF-8"))

(defn url-decode
  [s]
  (java.net.URLDecoder/decode s "UTF-8"))

(defn join-maps
  [& maps]
  (let [all-keys (apply set/union (for [m maps] (-> m keys set)))]
    (into {} (for [k all-keys]
               [k (for [m maps] (m k))]))))

(defn partition-fixed
  [max-num-chunks aseq]
  (if (zero? max-num-chunks)
    []
    (let [chunks (->> (integer-divided (count aseq) max-num-chunks)
                      (#(dissoc % 0))
                      (sort-by (comp - first))
                      (mapcat (fn [[size amt]] (repeat amt size)))
                      )]
      (loop [result []
             [chunk & rest-chunks] chunks
             data aseq]
        (if (nil? chunk)
          result
          (let [[c rest-data] (split-at chunk data)]
            (recur (conj result c)
                   rest-chunks
                   rest-data)))))))


(defn assoc-apply-self
  [curr key afn]
  (assoc curr key (afn curr)))

(defmacro recursive-map
  [& forms]
  (->> (partition 2 forms)
       (map (fn [[key form]] `(assoc-apply-self ~key (fn [~'<>] ~form))))
       (concat `(-> {}))))

(defn current-stack-trace
  []
  (->> (Thread/currentThread)
       .getStackTrace
       (map str)
       (str/join "\n")))

(defn get-iterator
  [^Iterable alist]
  (if alist (.iterator alist)))

(defn iter-has-next?
  [^Iterator iter]
  (if iter (.hasNext iter) false))

(defn iter-next
  [^Iterator iter]
  (.next iter))

(defmacro fast-list-iter
  [pairs & body]
  (let [pairs (partition 2 pairs)
        lists (map second pairs)
        elems (map first pairs)
        iters (map (fn [_] (gensym)) lists)
        bindings (->> (map (fn [i l] [i `(get-iterator ~l)]) iters lists)
                      (apply concat))
        tests (map (fn [i] `(iter-has-next? ~i)) iters)
        assignments (->> (map (fn [e i] [e `(iter-next ~i)]) elems iters)
                         (apply concat))]
    `(let [~@bindings]
       (while (and ~@tests)
         (let [~@assignments]
           ~@body)))))

(defn fast-list-map
  [afn alist]
  (let [ret (ArrayList.)]
    (fast-list-iter [e alist]
                    (.add ret (afn e)))
    ret))

(defmacro fast-list-for
  [[e alist] & body]
  `(fast-list-map (fn [~e] ~@body) ~alist))

(defn map-iter
  [^Map amap]
  (if amap (-> amap .entrySet .iterator)))

(defn convert-entry
  [^Map$Entry entry]
  [(.getKey entry) (.getValue entry)])

(defmacro fast-map-iter
  [[bind amap] & body]
  `(let [iter# (map-iter ~amap)]
     (while (iter-has-next? iter#)
       (let [entry# (iter-next iter#)
             ~bind (convert-entry entry#)]
         ~@body))))

(defn fast-first
  [^List alist]
  (.get alist 0))

(defmacro get-with-default
  [amap key default-val]
  `(let [curr# (.get ~amap ~key)]
     (if curr#
       curr#
       (do
         (let [new# ~default-val]
           (.put ~amap ~key new#)
           new#)))))

(defn fast-group-by
  [afn alist]
  (let [ret (HashMap.)]
    (fast-list-iter
      [e alist]
      (let [key (afn e)
            ^List curr (get-with-default ret key (ArrayList.))]
        (.add curr e)))
    ret))

(defn new-instance
  [klass]
  (let [klass (if (string? klass) (Class/forName klass) klass)]
    (.newInstance klass)))

(defmacro -<>
  ([x] x)
  ([x form] (if (seq? form)
              (with-meta
                (let [[begin [_ & end]] (split-with #(not= % '<>) form)]
                  (concat begin [x] end))
                (meta form))
              (list form x)))
  ([x form & more] `(-<> (-<> ~x ~form) ~@more)))

(defn hashmap-to-persistent [^HashMap m]
  (zipmap (.keySet m) (.values m)))
