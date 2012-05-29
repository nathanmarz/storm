(ns backtype.storm.daemon.acker
  (:import [backtype.storm.task OutputCollector TopologyContext IBolt])
  (:import [backtype.storm.tuple Tuple Fields])
  (:import [backtype.storm.utils TimeCacheMap])
  (:import [java.util List Map])
  (:use [backtype.storm config util log])
  (:gen-class
   :init init
   :implements [backtype.storm.task.IBolt]
   :constructors {[] []}
   :state state ))

(def ACKER-COMPONENT-ID "__acker")
(def ACKER-INIT-STREAM-ID "__ack_init")
(def ACKER-ACK-STREAM-ID "__ack_ack")
(def ACKER-FAIL-STREAM-ID "__ack_fail")

(defn- update-ack [curr-entry val]
  (let [old (get curr-entry :val 0)]
    (assoc curr-entry :val (bit-xor old val))
    ))

(defn- acker-emit-direct [^OutputCollector collector ^Integer task ^String stream ^List values]
  (.emitDirect collector task stream values)
  )

(defn mk-acker-bolt []
  (let [output-collector (atom nil)
        pending (atom nil)]
    (reify IBolt
      (^void prepare [this ^Map storm-conf ^TopologyContext context ^OutputCollector collector]
               (reset! output-collector collector)
               (reset! pending (TimeCacheMap. (.maxTopologyMessageTimeout context)))
               )
      (^void execute [this ^Tuple tuple]
             (let [id (.getValue tuple 0)
                   ^TimeCacheMap pending @pending
                   curr (.get pending id)
                   curr (condp = (.getSourceStreamId tuple)
                            ACKER-INIT-STREAM-ID (-> curr
                                                     (update-ack (.getValue tuple 1))
                                                     (assoc :spout-task (.getValue tuple 2)))
                            ACKER-ACK-STREAM-ID (update-ack curr (.getValue tuple 1))
                            ACKER-FAIL-STREAM-ID (assoc curr :failed true))]
               (.put pending id curr)
               (when (and curr
                          (:spout-task curr))
                 (cond (= 0 (:val curr))
                       (do
                         (.remove pending id)
                         (acker-emit-direct @output-collector
                                            (:spout-task curr)
                                            ACKER-ACK-STREAM-ID
                                            [id]
                                            ))
                       (:failed curr)
                       (do
                         (.remove pending id)
                         (acker-emit-direct @output-collector
                                            (:spout-task curr)
                                            ACKER-FAIL-STREAM-ID
                                            [id]
                                            ))
                       ))
               (.ack ^OutputCollector @output-collector tuple)
               ))
      (^void cleanup [this]
        (.cleanup @pending))
      )))

(defn -init []
  [[] (container)])

(defn -prepare [this conf context collector]
  (let [^IBolt ret (mk-acker-bolt)]
    (container-set! (.state this) ret)
    (.prepare ret conf context collector)
    ))

(defn -execute [this tuple]
  (let [^IBolt delegate (container-get (.state this))]
    (.execute delegate tuple)
    ))

(defn -cleanup [this]
  (let [^IBolt delegate (container-get (.state this))]
    (.cleanup delegate)
    ))
