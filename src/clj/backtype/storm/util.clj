(ns backtype.storm.util
  (:import [java.net InetAddress])
  (:import [java.util Map List Collection])
  (:import [java.io FileReader])
  (:import [backtype.storm Config])
  (:import [backtype.storm.utils Time Container ClojureTimerTask])
  (:import [java.util UUID])
  (:import [java.util.concurrent.locks ReentrantReadWriteLock])
  (:import [java.util.concurrent Semaphore])
  (:import [java.io File RandomAccessFile StringWriter PrintWriter])
  (:import [java.lang.management ManagementFactory])
  (:import [org.apache.commons.exec DefaultExecutor CommandLine])
  (:import [org.apache.commons.io FileUtils])
  (:import [org.apache.commons.exec ExecuteException])
  (:import [org.json.simple JSONValue])
  (:import [clojure.lang RT])
  (:require [clojure.contrib [str-utils2 :as str]])
  (:require [clojure [set :as set]])
  (:use [clojure walk])
  (:use [backtype.storm log])
  (:use [clojure.contrib.def :only [defnk]])
  )

(defn local-hostname []
  (.getCanonicalHostName (InetAddress/getLocalHost)))

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

(defn current-time-secs []
  (int (unchecked-divide (Time/currentTimeMillis) (long 1000))))

(defn clojurify-structure [s]
  (prewalk (fn [x]
              (cond (instance? Map x) (into {} x)
                    (instance? List x) (vec x)
                    true x))
           s))

(defmacro with-file-lock [path & body]
  `(let [f# (File. ~path)
         _# (.createNewFile f#)
         rf# (RandomAccessFile. f# "rw")
         lock# (.. rf# (getChannel) (lock))]
      (try
        ~@body
        (finally
          (.release lock#)
          (.close rf#))
        )))

(defn tokenize-path [^String path]
  (let [toks (.split path "/")]
    (vec (filter (complement empty?) toks))
    ))

(defn assoc-conj [m k v]
  (merge-with concat m {k [v]}))

;; returns [ones in first set not in second, ones in second set not in first]
(defn set-delta [old curr]
  (let [s1 (set old)
        s2 (set curr)]
    [(set/difference s1 s2) (set/difference s2 s1)]
    ))

(defn parent-path [path]
  (let [toks (tokenize-path path)]
    (str "/" (str/join "/" (butlast toks)))
    ))

(defn toks->path [toks]
  (str "/" (str/join "/" toks))
  )

(defn normalize-path [^String path]
  (toks->path (tokenize-path path)))

(defn map-val [afn amap]
  (into {}
    (for [[k v] amap]
      [k (afn v)]
      )))

(defn filter-val [afn amap]
  (into {}
    (filter
      (fn [[k v]]
        (afn v))
       amap
       )))

(defn full-path [parent name]
  (let [toks (tokenize-path parent)]
    (toks->path (conj toks name))
    ))

(defn not-nil? [o]
  (not (nil? o)))

(defn barr [& vals]
  (byte-array (map byte vals)))

(defn halt-process! [val & msg]
  (log-message "Halting process: " msg)
  (Thread/sleep 1000)
  (.halt (Runtime/getRuntime) val)
  )

(defn sum [vals]
  (reduce + vals))

(defn repeat-seq
  ([aseq]
    (apply concat (repeat aseq)))
  ([amt aseq]
    (apply concat (repeat amt aseq))
    ))

(defn div
  "Perform floating point division on the arguments."
  [f & rest] (apply / (double f) rest))

(defn defaulted [val default]
  (if val val default))

(defn mk-counter
  ([] (mk-counter 1))
  ([start-val]
     (let [val (atom (dec start-val))]
       (fn []
         (swap! val inc)))))

(defmacro for-times [times & body]
  `(for [i# (range ~times)]
     ~@body
     ))

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
    (first split)
    ))

(defn exec-command! [command]
  (let [[comm-str & args] (seq (.split command " "))
        command (CommandLine. comm-str)]
    (doseq [a args]
      (.addArgument command a))
    (.execute (DefaultExecutor.) command)
    ))

(defn extract-dir-from-jar [jarpath dir destdir]
  (try
    (exec-command! (str "unzip -qq " jarpath " " dir "/** -d " destdir))
  (catch ExecuteException e
    (log-message "Error when trying to extract " dir " from " jarpath))
  ))

(defn ensure-process-killed! [pid]
  ;; TODO: should probably do a ps ax of some sort to make sure it was killed
  (try
    (exec-command! (str "kill -9 " pid))
  (catch ExecuteException e
    (log-message "Error when trying to kill " pid ". Process is probably already dead."))
    ))

(defnk launch-process [command :environment {}]
  (let [command (seq (.split command " "))
        builder (ProcessBuilder. (cons "nohup" command))
        process-env (.environment builder)]
    (doseq [[k v] environment]
      (.put process-env k v))
    (.start builder)
    ))

(defn sleep-secs [secs]
  (Time/sleep (* (long secs) 1000)))

(defn sleep-until-secs [target-secs]
  (Time/sleepUntil (* (long target-secs) 1000)))

(defprotocol SmartThread
  (start [this])
  (join [this])
  (interrupt [this])
  (sleeping? [this]))

;; afn returns amount of time to sleep
(defnk async-loop [afn
                   :daemon false
                   :kill-fn (fn [error] (halt-process! 1 "Async loop died!"))
                   :priority Thread/NORM_PRIORITY
                   :args-fn (fn [] [])
                   :start true]
  (let [thread (Thread.
                (fn []
                  (try
                    (let [args (args-fn)]
                      (loop []
                        (let [sleep-time (apply afn args)]
                          (when-not (nil? sleep-time)
                            (sleep-secs sleep-time)
                            (recur))
                          )))
                    (catch InterruptedException e
                      (log-message "Async loop interrupted!")
                      )
                    (catch Throwable t
                      ;; work around clojure wrapping exceptions
                      (if (instance? InterruptedException (.getCause t))
                        (log-message "Async loop interrupted!")
                        (do
                          (log-error t "Async loop died!")
                          (kill-fn t)
                          ))
                      ))
                  ))]
    (.setDaemon thread daemon)
    (.setPriority thread priority)
    (when start
      (.start thread))
    ;; should return object that supports stop, interrupt, join, and waiting?
    (reify SmartThread
      (start [this]
        (.start thread))
      (join [this]
        (.join thread))
      (interrupt [this]
        (.interrupt thread))
      (sleeping? [this]
        (Time/isThreadWaiting thread)
        ))
      ))

(defn filter-map-val [afn amap]
  (into {} (filter (fn [[k v]] (afn v)) amap)))

(defn exists-file? [path]
  (.exists (File. path)))

(defn rmr [path]
  (log-debug "Rmr path " path)
  (when (exists-file? path)
    (FileUtils/forceDelete (File. path))))

(defn rmpath
  "Removes file or directory at the path. Not recursive. Throws exception on failure"
  [path]
  (log-debug "Removing path " path)
  (let [deleted? (.delete (File. path))]
    (when-not deleted?
      (throw (RuntimeException. (str "Failed to delete " path))))
    ))

(defn local-mkdirs
  [path]
  (log-debug "Making dirs at " path)
  (FileUtils/forceMkdir (File. path)))

(defn touch [path]
  (log-debug "Touching file at " path)
  (let [success? (.createNewFile (File. path))]
    (when-not success?
      (throw (RuntimeException. (str "Failed to touch " path))))
    ))

(defn read-dir-contents [dir]
  (if (exists-file? dir)
    (let [content-files (.listFiles (File. dir))]
      (map #(.getName ^File %) content-files))
    [] ))

(defn compact [aseq]
  (filter (complement nil?) aseq))

(defn current-classpath []
  (System/getProperty "java.class.path"))

(defn add-to-classpath [classpath paths]
  (str/join ":" (cons classpath paths)))

(defn ^ReentrantReadWriteLock mk-rw-lock []
  (ReentrantReadWriteLock.))

(defmacro read-locked [rw-lock & body]
  (let [lock (with-meta rw-lock {:tag `ReentrantReadWriteLock})]
    `(let [rlock# (.readLock ~lock)]
       (try (.lock rlock#)
            ~@body
            (finally (.unlock rlock#))))))

(defmacro write-locked [rw-lock & body]
  (let [lock (with-meta rw-lock {:tag `ReentrantReadWriteLock})]
    `(let [wlock# (.writeLock ~lock)]
       (try (.lock wlock#)
            ~@body
            (finally (.unlock wlock#))))))

(defn wait-for-condition [apredicate]
  (while (not (apredicate))
    (Time/sleep 100)
    ))

(defn some? [pred aseq]
  ((complement nil?) (some pred aseq)))

(defn time-delta [time-secs]
  (- (current-time-secs) time-secs))

(defn time-delta-ms [time-ms]
  (- (System/currentTimeMillis) time-ms))

(defn parse-int [str]
  (Integer/parseInt str))

(defn integer-divided [sum num-pieces]
  (let [base (int (/ sum num-pieces))
        num-inc (mod sum num-pieces)
        num-bases (- num-pieces num-inc)]
    (if (= num-inc 0)
      {base num-bases}
      {base num-bases (inc base) num-inc}
      )))

(defn collectify [obj]
  (if (or (sequential? obj) (instance? Collection obj)) obj [obj]))

(defn to-json [^Map m]
  (JSONValue/toJSONString m))

(defn from-json [^String str]
  (clojurify-structure
    (JSONValue/parse str)))

(defmacro letlocals [& body]
   (let [[tobind lexpr] (split-at (dec (count body)) body)
         binded (vec (mapcat (fn [e]
                  (if (and (list? e) (= 'bind (first e)))
                     [(second e) (last e)]
                     ['_ e]
                     ))
                  tobind ))]
     `(let ~binded
         ~(first lexpr)
      )))

(defn remove-first [pred aseq]
  (let [[b e] (split-with (complement pred) aseq)]
    (when (empty? e)
      (throw (IllegalArgumentException. "Nothing to remove")))
    (concat b (rest e))
    ))

(defn multi-set
  "Returns a map of elem to count"
  [aseq]
  (apply merge-with +
         (map #(hash-map % 1) aseq)))

(defn set-var-root* [avar val]
  (alter-var-root avar (fn [avar] val)))

(defmacro set-var-root [var-sym val]
  `(set-var-root* (var ~var-sym) ~val))

(defmacro with-var-roots [bindings & body]
  (let [settings (partition 2 bindings)
        tmpvars (repeatedly (count settings) (partial gensym "old"))
        vars (map first settings)
        savevals (vec (mapcat (fn [t v] [t v]) tmpvars vars))
        setters (for [[v s] settings] `(set-var-root ~v ~s))
        restorers (map (fn [v s] `(set-var-root ~v ~s)) vars tmpvars)
        ]
    `(let ~savevals
      ~@setters
      (try
        ~@body
      (finally
        ~@restorers))
      )))

(defn map-diff
  "Returns mappings in m2 that aren't in m1"
  [m1 m2]
  (into {}
    (filter
      (fn [[k v]] (not= v (m1 k)))
      m2
      )))


(defn select-keys-pred [pred amap]
  (into {}
        (filter
         (fn [[k v]]
           (pred k))
         amap)))


(defn rotating-random-range [amt]
  (ref (shuffle (range amt))))

(defn acquire-random-range-id [rr amt]
  (dosync
   (let [ret (first @rr)]
     (alter
      rr
      (fn [rr]
        (if (= 1 (count rr))
          (shuffle (range amt))
          (next rr))
        ))
     ret
     )))

; this can be rewritten to be tail recursive
(defn interleave-all [& colls]
  (if (empty? colls)
    []
    (let [colls (filter (complement empty?) colls)
          my-elems (map first colls)
          rest-elems (apply interleave-all (map rest colls))]
      (concat my-elems rest-elems)
      )))

(defn update [m k afn]
  (assoc m k (afn (get m k))))

(defn any-intersection [& sets]
  (let [elem->count (multi-set (apply concat sets))]
    (-> (filter-val #(> % 1) elem->count)
        keys
        )))

(defn between?
  "val >= lower and val <= upper"
  [val lower upper]
  (and (>= val lower)
       (<= val upper)))

(defmacro benchmark [& body]
  `(time
    (doseq [i# (range 1000000)]
      ~@body)))

(defn rand-sampler [freq]
  (let [r (java.util.Random.)]
    (fn []
      (= 0 (.nextInt r freq)))
    ))

(defn even-sampler [freq]
  (let [r (java.util.Random.)
        state (atom [-1 (.nextInt r freq)])
        updater (fn [[i target]]
                  (let [i (inc i)]
                    (if (>= i freq)
                      [0 (.nextInt r freq)]
                      [i target]
                      )))]
    (with-meta
      (fn []
        (let [[i target] (swap! state updater)]
          (= i target)
          ))
      {:rate freq})))

(defn sampler-rate [sampler]
  (:rate (meta sampler)))

(defn class-selector [obj & args] (class obj))

(defn uptime-computer []
  (let [start-time (current-time-secs)]
    (fn []
      (time-delta start-time)
      )))

(defn stringify-error [error]
  (let [result (StringWriter.)
        printer (PrintWriter. result)]
    (.printStackTrace error printer)
    (.toString result)
    ))

(defn nil-to-zero [v]
  (if v v 0))

(defn bit-xor-vals [vals]
  (reduce bit-xor 0 vals))

(defmacro with-error-reaction [afn & body]
  `(try ~@body
     (catch Throwable t# (~afn t#))))

(defn container []
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

(defn exception-cause? [klass ^Throwable t]
  (->> (iterate #(.getCause ^Throwable %) t)
       (take-while identity)
       (some (partial instance? klass))
       boolean))

(defmacro forcat [[args aseq] & body]
  `(mapcat (fn [~args]
             ~@body)
           ~aseq))

(defmacro try-cause [& body]
  (let [checker (fn [form]
                  (or (not (sequential? form))
                      (not= 'catch (first form))))
        [code guards] (split-with checker body)
        error-local (gensym "t")
        guards (forcat [[_ klass local & guard-body] guards]
                 `((exception-cause? ~klass ~error-local)
                   (let [~local ~error-local]
                     ~@guard-body
                     )))
        ]
    `(try ~@code
          (catch Throwable ~error-local
            (cond ~@guards
                  true (throw ~error-local)
                  )))))

(defn redirect-stdio-to-log4j! []
  ;; set-var-root doesn't work with *out* and *err*, so digging deeper here
  (.set RT/OUT (java.io.OutputStreamWriter.
                 (log-stream :info "STDIO")))
  (.set RT/ERR (PrintWriter.
                 (java.io.OutputStreamWriter.
                   (log-stream :error "STDIO"))
                   true))
  (log-capture! "STDIO"))
