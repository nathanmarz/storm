(ns backtype.storm.clojure
  (:use [backtype.storm bootstrap util])
  (:import [backtype.storm StormSubmitter])
  (:import [backtype.storm.generated StreamInfo])
  (:import [backtype.storm.tuple Tuple])
  (:import [backtype.storm.task OutputCollector Ibolth TopologyContext])
  (:import [backtype.storm.spout SpoutOutputCollector ISpout])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm.clojure Clojurebolth ClojureSpout])
  (:import [java.util List])
  (:require [backtype.storm [thrift :as thrift]]))


(defn direct-stream [fields]
  (StreamInfo. fields true))

(defn to-spec [avar]
  (let [m (meta avar)]
    [(str (:ns m)) (str (:name m))]))

(defn clojure-bolth* [output-spec fn-var conf-fn-var args]
  (Clojurebolth. (to-spec fn-var) (to-spec conf-fn-var) args (thrift/mk-output-spec output-spec)))

(defmacro clojure-bolth [output-spec fn-sym conf-fn-sym args]
  `(clojure-bolth* ~output-spec (var ~fn-sym) (var ~conf-fn-sym) ~args))

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

(defmacro bolth [& body]
  (let [[bolth-fns other-fns] (split-with #(not (symbol? %)) body)
        fns (normalize-fns bolth-fns)]
    `(reify Ibolth
       ~@fns
       ~@other-fns)))

(defmacro bolth-execute [& body]
  `(bolth
     (~'execute ~@body)))

(defmacro spout [& body]
  (let [[spout-fns other-fns] (split-with #(not (symbol? %)) body)
        fns (normalize-fns spout-fns)]
    `(reify ISpout
       ~@fns
       ~@other-fns)))

(defmacro defbolth [name output-spec & [opts & impl :as all]]
  (if-not (map? opts)
    `(defbolth ~name ~output-spec {} ~@all)
    (let [worker-name (symbol (str name "__"))
          conf-fn-name (symbol (str name "__conf__"))
          params (:params opts)
          conf-code (:conf opts)
          fn-body (if (:prepare opts)
                    (cons 'fn impl)
                    (let [[args & impl-body] impl
                          coll-sym (nth args 1)
                          args (vec (take 1 args))
                          prepargs [(gensym "conf") (gensym "context") coll-sym]]
                      `(fn ~prepargs (bolth (~'execute ~args ~@impl-body)))))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-bolth ~output-spec ~worker-name ~conf-fn-name args#))
                    `(def ~name
                       (clojure-bolth ~output-spec ~worker-name ~conf-fn-name []))
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

(defnk emit-bolth! [collector values
                   :stream Utils/DEFAULT_STREAM_ID :anchor []]
  (let [^List anchor (collectify anchor)
        values (tuple-values values collector stream) ]
    (.emit ^OutputCollector (:output-collector collector) stream anchor values)
    ))

(defnk emit-direct-bolth! [collector task values
                          :stream Utils/DEFAULT_STREAM_ID :anchor []]
  (let [^List anchor (collectify anchor)
        values (tuple-values values collector stream) ]
    (.emitDirect ^OutputCollector (:output-collector collector) task stream anchor values)
    ))

(defn ack! [collector ^Tuple tuple]
  (.ack ^OutputCollector (:output-collector collector) tuple))

(defn fail! [collector ^Tuple tuple]
  (.fail ^OutputCollector (:output-collector collector) tuple))

(defnk emit-spout! [collector values
                    :stream Utils/DEFAULT_STREAM_ID :id nil]
  (let [values (tuple-values values collector stream)]
    (.emit ^SpoutOutputCollector (:output-collector collector) stream values id)))

(defnk emit-direct-spout! [collector task values
                           :stream Utils/DEFAULT_STREAM_ID :id nil]
  (let [values (tuple-values values collector stream)]
    (.emitDirect ^SpoutOutputCollector (:output-collector collector) task stream values id)))

(defalias topology thrift/mk-topology)
(defalias bolth-spec thrift/mk-bolth-spec)
(defalias spout-spec thrift/mk-spout-spec)
(defalias shell-bolth-spec thrift/mk-shell-bolth-spec)
(defalias shell-spout-spec thrift/mk-shell-spout-spec)

(defn submit-remote-topology [name conf topology]
  (StormSubmitter/submitTopology name conf topology))

(defn local-cluster []  
  ;; do this to avoid a cyclic dependency of
  ;; LocalCluster -> testing -> nimbus -> bootstrap -> clojure -> LocalCluster
  (eval '(new backtype.storm.LocalCluster)))
