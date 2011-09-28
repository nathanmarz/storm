(ns backtype.storm.clojure
  (:use backtype.storm.bootstrap)
  (:import [backtype.storm.generated StreamInfo])
  (:import [backtype.storm.tuple Tuple])
  (:import [backtype.storm.task OutputCollector IBolt])
  (:import backtype.storm.clojure.ClojureBolt)
  (:require [backtype.storm [thrift :as thrift]]))

(defn direct-stream [fields]
  (StreamInfo. fields true))

(defn clojure-bolt* [output-spec fn-var args]
  (let [m (meta fn-var)]
    (ClojureBolt. (str (:ns m)) (str (:name m)) args (thrift/mk-output-spec output-spec))
    ))

(defmacro clojure-bolt [output-spec fn-sym args]
  `(clojure-bolt* ~output-spec (var ~fn-sym) ~args))

(defmacro bolt [& body]
  (let [fns (for [[name args & impl] body
                  :let [args (-> "this"
                                 gensym
                                 (cons args)
                                 vec)]]
              (concat [name args] impl)
              )]
    `(reify IBolt
       ~@fns)))

(defmacro defbolt [name output-spec & [opts & impl :as all]]
  (if-not (map? opts)
    `(defbolt ~name ~output-spec {} ~@all)
    (let [worker-name (symbol (str name "__"))
          params (:params opts)
          fn-body (if (:prepare opts)
                    (cons 'fn impl)
                    (let [[args & impl-body] impl
                          coll-sym (nth args 1)
                          args (vec (take 1 args))]
                      `(fn [conf# context# ~coll-sym] (bolt (~'execute ~args ~@impl-body)))))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-bolt ~output-spec ~worker-name args#))
                    `(def ~name
                       (clojure-bolt ~output-spec ~worker-name []))
                    )
          ]
      `(do
         (defn ~worker-name ~(if params params [])
           ~fn-body
           )
         ~definer
         )
      )))
