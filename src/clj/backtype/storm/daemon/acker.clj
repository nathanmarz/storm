(ns backtype.storm.daemon.acker
  (:import [backtype.storm.task OutputCollector TopologyContext])
  (:import [backtype.storm.tuple Tuple Fields])
  (:import [backtype.storm.utils TimeCacheMap])
  (:import [backtype.storm.topology IRichBolt])
  (:import [java.util List Map])
  (:use [backtype.storm config]))

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
    (reify IRichBolt
      (^void prepare [this ^Map storm-conf ^TopologyContext context ^OutputCollector collector]
               (reset! output-collector collector)
               (reset! pending (TimeCacheMap. (int (storm-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS))))
               )
      (^void execute [this ^Tuple tuple]
             (let [id (.getValue tuple 0)
                   ^TimeCacheMap pending @pending
                   curr (.get pending id)
                   curr (condp = (.getSourceStreamId tuple)
                            ACKER-INIT-STREAM-ID (-> curr
                                                     (update-ack id)
                                                     (assoc :spout-task (.getValue tuple 1)))
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
             )
      (declareOutputFields [this declarer]
        (.declareStream declarer ACKER-ACK-STREAM-ID true (Fields. ["id"]))
        (.declareStream declarer ACKER-FAIL-STREAM-ID true (Fields. ["id"])))
      )))
