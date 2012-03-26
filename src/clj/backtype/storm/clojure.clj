(ns backtype.storm.clojure
  (:use [clojure.contrib.def :only [defnk defalias]])
  (:use [clojure.contrib.seq-utils :only [find-first]])
  (:use [backtype.storm bootstrap util])
  (:import [backtype.storm StormSubmitter])
  (:import [backtype.storm.generated StreamInfo])
  (:import [backtype.storm.tuple Tuple])
  (:import [backtype.storm.task OutputCollector IBolt TopologyContext])
  (:import [backtype.storm.spout SpoutOutputCollector ISpout])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm.clojure ClojureBolt ClojureSpout])
  (:require [backtype.storm [thrift :as thrift]]))


(defn direct-stream [fields]
  (StreamInfo. fields true))

(defn to-spec [avar]
  (let [m (meta avar)]
    [(str (:ns m)) (str (:name m))]))

(defn clojure-bolt* [output-spec fn-var conf-fn-var args]
  (ClojureBolt. (to-spec fn-var) (to-spec conf-fn-var) args (thrift/mk-output-spec output-spec)))

(defmacro clojure-bolt [output-spec fn-sym conf-fn-sym args]
  `(clojure-bolt* ~output-spec (var ~fn-sym) (var ~conf-fn-sym) ~args))

(defn clojure-spout* [output-spec fn-var conf-var args]
  (let [m (meta fn-var)]
    (ClojureSpout. (to-spec fn-var) (to-spec conf-var) args (thrift/mk-output-spec output-spec))
    ))

(defmacro clojure-spout [output-spec fn-sym conf-sym args]
  `(clojure-spout* ~output-spec (var ~fn-sym) (var ~conf-sym) ~args))

(defn normalize-fns [body]
  (for [[name args & impl] body
        :let [args (-> "this"
                       gensym
                       (cons args)
                       vec)]]
    (concat [name args] impl)
    ))

(defn mk-concise-reify [klass body]
  (let [[reify-fns other-fns] (split-with #(not (symbol? %)) body)
        fns (normalize-fns reify-fns)]
    `(reify ~klass
       ~@fns
       ~@other-fns)))

(defmacro bolt [& body]
  (mk-concise-reify 'IBolt body))

(defmacro spout [& body]
  (mk-concise-reify 'ISpout body))

{:keys [url service_description]
      :or {service_description "Ganglia Web Frontend"}
      :as options}

(defn gen-prep-args [klass-sym prep-sym collector-sym]
  (let [params (->> klass-sym
                    resolve
                    .getMethods
                    (filter #(= (.getName %) (str prep-sym)))
                    first
                    .getParameterTypes)]
    (for [p params]
      (if (.contains (.getSimpleName p) "Collector")
        collector-sym
        (gensym)
        ))))

(defn mk-delegate-class [name main-interface prepare-method delegate-maker committer?]
  (let [methods (-> main-interface resolve .getMethods)
        interfaces (if committer? [main-interface 'ICommitter] [main-interface])
        prefix (str name "-")
        implementations (for [m methods]
                          (let []
                          `(defn ~(str prefix (.getName m))
                            )))]
    `(do
       ;; TODO: Might need to auto-ns qualify name
       ;; need a constructor whichs stores an object reference
       ;; need to type hint the object as "main-interface" to avoid reflection
       (gen-class
         :name ~name
         :implements ~interfaces
         :prefix ~prefix)
        ~@implementations
        )))

;; TODO: need something else that will auto-gen "bolt", "spout"
;; want it to work for batchbolt, richbolt, richspout, basicbolt
;; need to be able to add extra interfaces
(defn mk-executor [main-interface ;; e.g., IRichBolt, IRichSpout
                   prepare-method
                   reify-sym
                   [name output-spec & [opts & impl :as all]]
                   & {:keys [default-method prepare-default]
                      :or [prepare-default true]
                      :as executor-opts}]
  (if-not (map? opts)
    (apply mk-executor main-interface prepare-method reify-sym
           (concat [name output-spec {}] all)
           (mapcat identity executor-opt))
    (let [worker-name (symbol (str name "__"))
          conf-fn-name (symbol (str name "__conf__"))
          params (:params opts)
          conf-code (:conf opts)
          committer? (:committer opts) ;; kind of a hack...
          prepare? (if (contains? opts :prepare) (:prepare opts) prepare-default)
          fn-body (if prepare?
                    (cons 'fn impl)
                    (let [[args & impl-body] impl
                          coll-sym (last args)
                          args (butlast args)
                          prepargs (gen-prep-args main-interface prepare-method coll-sym)]
                      `(fn ~(vec prepargs) (~reify-sym (~default-method ~(vec args) ~@impl-body)))))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-bolt ~output-spec ~worker-name ~conf-fn-name args#))
                    `(def ~name
                       (clojure-bolt ~output-spec ~worker-name ~conf-fn-name []))
                    )
          ]
      `(do
        ;; TODO: need to do a gen-class for this object using "name"
         (defn ~conf-fn-name ~(if params params [])
           ~conf-code
           )
         (defn ~worker-name ~(if params params [])
           ~fn-body
           )
         ~definer
         ))))
(defmacro defbolt [name output-spec & [opts & impl :as all]]
  (if-not (map? opts)
    `(defbolt ~name ~output-spec {} ~@all)
    (let [worker-name (symbol (str name "__"))
          conf-fn-name (symbol (str name "__conf__"))
          params (:params opts)
          conf-code (:conf opts)
          fn-body (if (:prepare opts)
                    (cons 'fn impl)
                    (let [[args & impl-body] impl
                          coll-sym (nth args 1)
                          args (vec (take 1 args))
                          ;; unify by taking collector as last arg 
                          ;; (batch bolt should force prepare...)
                          ;; need options to implement other interfaces
                          prepargs [(gensym "conf") (gensym "context") coll-sym]]
                      `(fn ~prepargs (bolt (~'execute ~args ~@impl-body)))))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-bolt ~output-spec ~worker-name ~conf-fn-name args#))
                    `(def ~name
                       (clojure-bolt ~output-spec ~worker-name ~conf-fn-name []))
                    )
          ]
      `(do
         (defn ~conf-fn-name ~(if params params [])
           ~conf-code
           )
         (defn ~worker-name ~(if params params [])
           ~fn-body
           )
         ~definer
         ))))

(defmacro defspout [name output-spec & [opts & impl :as all]]
  (if-not (map? opts)
    `(defspout ~name ~output-spec {} ~@all)
    (let [worker-name (symbol (str name "__"))
          conf-fn-name (symbol (str name "__conf__"))
          params (:params opts)
          conf-code (:conf opts)
          prepare? (:prepare opts)
          prepare? (if (nil? prepare?) true prepare?)
          fn-body (if prepare?
                    (cons 'fn impl)
                    (let [[args & impl-body] impl
                          coll-sym (first args)
                          prepargs [(gensym "conf") (gensym "context") coll-sym]]
                      `(fn ~prepargs (spout (~'nextTuple [] ~@impl-body)))))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-spout ~output-spec ~worker-name ~conf-fn-name args#))
                    `(def ~name
                       (clojure-spout ~output-spec ~worker-name ~conf-fn-name []))
                    )
          ]
      `(do
         (defn ~conf-fn-name ~(if params params [])
           ~conf-code
           )
         (defn ~worker-name ~(if params params [])
           ~fn-body
           )
         ~definer
         ))))

(defprotocol TupleValues
  (tuple-values [values collector stream]))

(extend-protocol TupleValues
  java.util.Map
  (tuple-values [this collector ^String stream]
    (let [^TopologyContext context (:context collector)
          fields (..  context (getThisOutputFields stream) toList) ]
      (vec (map (into 
                  (empty this) (for [[k v] this] 
                                   [(if (keyword? k) (name k) k) v])) 
                fields))))
  java.util.List
  (tuple-values [this collector stream]
    this))

(defnk emit-bolt! [collector ^TupleValues values
                   :stream Utils/DEFAULT_STREAM_ID :anchor []]
  (let [^List anchor (collectify anchor)
        values (tuple-values values collector stream) ]
    (.emit ^OutputCollector (:output-collector collector) stream anchor values)
    ))

(defnk emit-direct-bolt! [collector task ^TupleValues values
                          :stream Utils/DEFAULT_STREAM_ID :anchor []]
  (let [^List anchor (collectify anchor)
        values (tuple-values values collector stream) ]
    (.emitDirect ^OutputCollector (:output-collector collector) task stream anchor values)
    ))

(defn ack! [collector ^Tuple tuple]
  (.ack ^OutputCollector (:output-collector collector) tuple))

(defn fail! [collector ^Tuple tuple]
  (.fail ^OutputCollector (:output-collector collector) tuple))

(defnk emit-spout! [collector ^TupleValues values
                    :stream Utils/DEFAULT_STREAM_ID :id nil]
  (let [values (tuple-values values collector stream)]
    (.emit ^SpoutOutputCollector (:output-collector collector) stream values id)))

(defnk emit-direct-spout! [collector task ^TupleValues values
                           :stream Utils/DEFAULT_STREAM_ID :id nil]
  (let [values (tuple-values values collector stream)]
    (.emitDirect ^SpoutOutputCollector (:output-collector collector) task stream values id)))

(defalias topology thrift/mk-topology)
(defalias bolt-spec thrift/mk-bolt-spec)
(defalias spout-spec thrift/mk-spout-spec)
(defalias shell-bolt-spec thrift/mk-shell-bolt-spec)
(defalias shell-spout-spec thrift/mk-shell-spout-spec)

(defn submit-remote-topology [name conf topology]
  (StormSubmitter/submitTopology name conf topology))

(defn local-cluster []  
  ;; do this to avoid a cyclic dependency of
  ;; LocalCluster -> testing -> nimbus -> bootstrap -> clojure -> LocalCluster
  (eval '(new backtype.storm.LocalCluster)))
