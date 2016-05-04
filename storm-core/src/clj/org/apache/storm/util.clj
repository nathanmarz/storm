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

(ns org.apache.storm.util
  (:import [java.net InetAddress])
  (:import [java.util Map Map$Entry List ArrayList Collection Iterator HashMap])
  (:import [java.io FileReader FileNotFoundException])
  (:import [java.nio.file Paths])
  (:import [org.apache.storm Config])
 (:import [org.apache.storm.generated ErrorInfo])
  (:import [org.apache.storm.utils Time ClojureTimerTask Utils
            MutableObject])
  (:import [org.apache.storm.security.auth NimbusPrincipal])
  (:import [javax.security.auth Subject])
  (:import [java.util UUID Random ArrayList List Collections])
  (:import [java.util.zip ZipFile])
  (:import [java.util.concurrent.locks ReentrantReadWriteLock])
  (:import [java.util.concurrent Semaphore])
  (:import [java.nio.file Files Paths])
  (:import [java.nio.file.attribute FileAttribute])
  (:import [java.io File FileOutputStream RandomAccessFile StringWriter
            PrintWriter BufferedReader InputStreamReader IOException])
  (:import [java.lang.management ManagementFactory])
  (:import [org.apache.commons.exec DefaultExecutor CommandLine])
  (:import [org.apache.commons.io FileUtils])
  (:import [org.apache.commons.exec ExecuteException])
  (:import [org.json.simple JSONValue])
  (:import [org.yaml.snakeyaml Yaml]
           [org.yaml.snakeyaml.constructor SafeConstructor])
  (:require [clojure [string :as str]])
  (:import [clojure.lang RT])
  (:require [clojure [set :as set]])
  (:require [clojure.java.io :as io])
  (:use [clojure walk])
  (:require [ring.util.codec :as codec])
  (:use [org.apache.storm log]))

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

(defmacro thrown-cause?
  [klass & body]
  `(try
     ~@body
     false
     (catch Throwable t#
       (Utils/exceptionCauseIsInstanceOf ~klass t#))))

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
                       `((Utils/exceptionCauseIsInstanceOf ~klass ~error-local)
                         (let [~local ~error-local]
                           ~@guard-body
                           )))]
    `(try ~@code
       (catch Throwable ~error-local
         (cond ~@guards
               true (throw ~error-local)
               )))))

(defn clojurify-structure
  [s]
  (prewalk (fn [x]
             (cond (instance? Map x) (into {} x)
                   (instance? List x) (vec x)
                   ;; (Boolean. false) does not evaluate to false in an if.
                   ;; This fixes that.
                   (instance? Boolean x) (boolean x)
                   true x))
           s))
; move this func form convert.clj due to cyclic load dependency
(defn clojurify-error [^ErrorInfo error]
  (if error
    {
      :error (.get_error error)
      :time-secs (.get_error_time_secs error)
      :host (.get_host error)
      :port (.get_port error)
      }
    ))

;TODO: We're keeping this function around until all the code using it is properly tranlated to java
;TODO: by properly having the for loop IN THE JAVA FUNCTION that originally used this function.
(defn map-val
  [afn amap]
  (into {}
        (for [[k v] amap]
          [k (afn v)])))

;TODO: We're keeping this function around until all the code using it is properly tranlated to java
;TODO: by properly having the for loop IN THE JAVA FUNCTION that originally used this function.
(defn filter-val
  [afn amap]
  (into {} (filter (fn [[k v]] (afn v)) amap)))

;TODO: We're keeping this function around until all the code using it is properly tranlated to java
;TODO: by properly having the for loop IN THE JAVA FUNCTION that originally used this function.
(defn filter-key
  [afn amap]
  (into {} (filter (fn [[k v]] (afn k)) amap)))

;TODO: We're keeping this function around until all the code using it is properly tranlated to java
;TODO: by properly having the for loop IN THE JAVA FUNCTION that originally used this function.
(defn map-key
  [afn amap]
  (into {} (for [[k v] amap] [(afn k) v])))

;TODO: Once all the other clojure functions (100+ locations) are translated to java, this function becomes moot.
(def not-nil? (complement nil?))

(defmacro dofor [& body]
  `(doall (for ~@body)))

;; The following two will go away when worker, task, executor go away.
(defn assoc-apply-self [curr key afn]
  (assoc curr key (afn curr)))

; These seven following will go away later. To be replaced by idiomatic java.
(defmacro recursive-map
  [& forms]
    (->> (partition 2 forms)
         (map (fn [[key form]] `(assoc-apply-self ~key (fn [~'<>] ~form))))
         (concat `(-> {}))))

(defmacro fast-list-iter
  [pairs & body]
  (let [pairs (partition 2 pairs)
        lists (map second pairs)
        elems (map first pairs)
        iters (map (fn [_] (gensym)) lists)
        bindings (->> (map (fn [i l] (let [lg (gensym)] [lg l i `(if ~lg (.iterator ~lg))])) iters lists)
                      (apply concat))
        tests (map (fn [i] `(and ~i (.hasNext ^Iterator ~i))) iters)
        assignments (->> (map (fn [e i] [e `(.next ^Iterator ~i)]) elems iters)
                         (apply concat))]
    `(let [~@bindings]
       (while (and ~@tests)
         (let [~@assignments]
           ~@body)))))

(defmacro fast-list-for
  [[e alist] & body]
  `(let [ret# (ArrayList.)]
     (fast-list-iter [~e ~alist]
                     (.add ret# (do ~@body)))
     ret#))

(defmacro fast-map-iter
  [[bind amap] & body]
  `(let [iter# (if ~amap (.. ^Map ~amap entrySet iterator))]
     (while (and iter# (.hasNext ^Iterator iter#))
       (let [entry# (.next ^Iterator iter#)
             ~bind [(.getKey ^Map$Entry entry#) (.getValue ^Map$Entry entry#)]]
         ~@body))))

(defn fast-group-by
  [afn alist]
  (let [ret (HashMap.)]
    (fast-list-iter
      [e alist]
      (let [key (afn e)
            ^List curr (let [curr (.get ret key)]
                         (if curr
                           curr
                           (let [default (ArrayList.)]
                             (.put ret key default)
                             default)))]
        (.add curr e)))
    ret))

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
