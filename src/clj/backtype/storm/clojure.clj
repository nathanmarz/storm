(ns backtype.storm.clojure
  (:use backtype.storm.bootstrap)
  (:import [backtype.storm.generated StreamInfo])
  (:import [backtype.storm.tuple Tuple])
  (:import [backtype.storm.task OutputCollector])
  (:import backtype.storm.clojure.ClojureBolt)
  (:require [backtype.storm [thrift :as thrift]]))

(defn direct-stream [fields]
  (StreamInfo. fields true))

(defn clojure-bolt* [output-spec fn-var & args]
  (let [m (meta fn-var)]
    (ClojureBolt. (str (:ns m)) (str (:name m)) args (thrift/mk-output-spec output-spec))
    ))

(defmacro clojure-bolt [output-spec fn-sym & args]
  `(clojure-bolt* ~output-spec (var ~fn-sym) ~@args))

(defmacro defbolt [name output-spec [tuple-sym collector-sym] & body]
  (let [worker-name (symbol (str name "__"))]
    `(do
       (defn ~worker-name []
         (fn [^Tuple ~tuple-sym ^OutputCollector ~collector-sym]
           ~@body
           ))
       (def ~name (clojure-bolt ~output-spec ~worker-name))
       )))

(defn hint [sym class-sym]
  (with-meta sym {:tag class-sym})
  )

(defmulti hinted-args (fn [kw args] kw))

(defmethod hinted-args :prepare [_ [conf context collector]]
           [(hint conf 'java.util.Map)
            (hint context 'backtype.storm.task.TopologyContext)
            (hint collector 'backtype.storm.bolt.OutputCollector)]
           )

(defmethod hinted-args :execute [_ [tuple collector]]
           [(hint tuple 'backtype.storm.tuple.Tuple)
            (hint collector 'backtype.storm.task.OutputCollector)]
           )

(defmethod hinted-args :cleanup [_ [collector]]
           [(hint collector 'backtype.storm.task.OutputCollector)]
           )

(defmacro defboltfull [name output-spec & kwargs]
  (let [opts (apply hash-map kwargs)
        worker-name (symbol (str name "__"))
        let-bindings (:let opts)
        hof-args (:params opts)
        definer (if hof-args
                  `(defn ~name [& args#]
                     (apply clojure-bolt* ~output-spec (var ~worker-name) args#))
                  `(def ~name (clojure-bolt ~output-spec ~worker-name)))
        fns (select-keys opts [:prepare :execute :cleanup])
        fns (into {}
                  (for [[fn-kw [args & impl]] fns]
                    [fn-kw `(fn ~(hinted-args fn-kw args) ~@impl)]
                    ))]
    `(do
       (defn ~worker-name [~@hof-args]
         (let [~@let-bindings]
           ~fns
           ))
       ~definer
       )))
