(ns backtype.storm.clojure
  (:use [clojure.contrib.def :only [defnk]])
  (:use [backtype.storm bootstrap util])
  (:import [backtype.storm.generated StreamInfo])
  (:import [backtype.storm.tuple Tuple])
  (:import [backtype.storm.task OutputCollector IBolt])
  (:import [backtype.storm.utils Utils])
  (:import backtype.storm.clojure.ClojureBolt)
  (:require [backtype.storm [thrift :as thrift]]))

(defn hint [sym class-sym]
  (with-meta sym {:tag class-sym})
  )

(defmulti hinted-args (fn [m args] m))

(defmethod hinted-args 'prepare [_ [conf context collector]]
           [(hint conf 'java.util.Map)
            (hint context 'backtype.storm.task.TopologyContext)
            (hint collector 'backtype.storm.task.OutputCollector)]
           )

(defmethod hinted-args 'execute [_ [tuple]]
           [(hint tuple 'backtype.storm.tuple.Tuple)]
           )

(defmethod hinted-args 'cleanup [_ []]
           []
           )

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
                  :let [name (hint name 'void)
                        args (hinted-args name args)
                        args (-> "this"
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
                          args (vec (take 1 args))
                          prepargs (hinted-args 'prepare [(gensym "conf") (gensym "context") coll-sym])]
                      `(fn ~prepargs (bolt (~'execute ~args ~@impl-body)))))
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

(defnk emit! [^OutputCollector collector ^List values :stream Utils/DEFAULT_STREAM_ID :anchor []]
  (let [^List anchor (collectify anchor)]
    (.emit collector stream (collectify anchor) values)))

(defnk emit-direct! [^OutputCollector collector task ^List values :stream Utils/DEFAULT_STREAM_ID :anchor []]
  (let [^List anchor (collectify anchor)]
    (.emitDirect collector task stream (collectify anchor) values)))

(defn ack! [^OutputCollector collector ^Tuple tuple]
  (.ack collector tuple))
