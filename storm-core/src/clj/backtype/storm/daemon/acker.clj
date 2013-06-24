(ns backtype.storm.daemon.acker
  (:import [backtype.storm.task OutputCollector TopologyContext IBolt])
  (:import [backtype.storm.tuple Tuple Fields])
  (:import [backtype.storm.utils RotatingMap MutableObject])
  (:import [java.util List Map])
  (:import [backtype.storm Constants])
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

;;; The acker tracks completion of each tuple tree with a checksum hash: each
;;; time a tuple is sent, its value is XORed into the checksum, and each time a
;;; tuple is acked its value is XORed in again. If all tuples have been
;;; successfully acked, the checksum will be zero (and the odds that the
;;; checksum will be zero otherwise are vanishingly small).
;;;
;;; When the tuple tree is born, the spout seeds the ledger with the XORed
;;; edge-ids of each tuple recipient. Every time an executor acks, it sends back
;;; a partial checksum that is the XOR of the tuple's own edge-id (clearing it
;;; from the ledger) and the edge-id of each downstream tuple (thus entering
;;; them into the ledger).
;;;
;;; This method proceeds as follows.
;;;
;;; On a tick tuple, just advance pending checksums towards death and
;;; return. The RotatingMap will invoke the expiration callback for all the
;;; over-stale tuple trees.
;;;
;;; Otherwise, update or create the record for this tuple tree:
;;;
;;; * on init: initialize with the given checksum value, and record the spout's id for later
;;; * on ack:  xor the partial checksum into the existing checksum value
;;; * on fail: just mark it as failed
;;;
;;; Next, take action:
;;;
;;; * if the total checksum is zero, the tuple tree is complete:
;;;   remove it from the pending collection and notify the spout of success
;;; * if the tuple tree has failed, it is also complete:
;;;   remove it from the pending collection and notify the spout of failure
;;;
;;; Finally, pass on an ack of our own.
;;;
(defn mk-acker-bolt []
  (let [output-collector (MutableObject.)
        pending (MutableObject.)]
    (reify IBolt
      (^void prepare [this ^Map storm-conf ^TopologyContext context ^OutputCollector collector]
               (.setObject output-collector collector)
               (.setObject pending (RotatingMap. 2))
               )
      (^void execute [this ^Tuple tuple]
             (let [^RotatingMap pending (.getObject pending)
                   stream-id (.getSourceStreamId tuple)]
               (if (= stream-id Constants/SYSTEM_TICK_STREAM_ID)
                 (.rotate pending)
                 (let [id (.getValue tuple 0)
                       ^OutputCollector output-collector (.getObject output-collector)
                       curr (.get pending id)
                       curr (condp = stream-id
                                ACKER-INIT-STREAM-ID (-> curr
                                                         (update-ack (.getValue tuple 1))
                                                         (assoc :spout-task (.getValue tuple 2)))
                                ACKER-ACK-STREAM-ID (update-ack curr (.getValue tuple 1))
                                ACKER-FAIL-STREAM-ID (assoc curr :failed true))]
                   (.put pending id curr)
                   (when (and curr (:spout-task curr))
                     (cond (= 0 (:val curr))
                           (do
                             (.remove pending id)
                             (acker-emit-direct output-collector
                                                (:spout-task curr)
                                                ACKER-ACK-STREAM-ID
                                                [id]
                                                ))
                           (:failed curr)
                           (do
                             (.remove pending id)
                             (acker-emit-direct output-collector
                                                (:spout-task curr)
                                                ACKER-FAIL-STREAM-ID
                                                [id]
                                                ))
                           ))
                   (.ack output-collector tuple)
                   ))))
      (^void cleanup [this]
        )
      )))

(defn -init []
  [[] (container)])

(defn -prepare [this conf context collector]
  (let [^IBolt ret (mk-acker-bolt)]
    (container-set! (.state ^backtype.storm.daemon.acker this) ret)
    (.prepare ret conf context collector)
    ))

(defn -execute [this tuple]
  (let [^IBolt delegate (container-get (.state ^backtype.storm.daemon.acker this))]
    (.execute delegate tuple)
    ))

(defn -cleanup [this]
  (let [^IBolt delegate (container-get (.state ^backtype.storm.daemon.acker this))]
    (.cleanup delegate)
    ))
