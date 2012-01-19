(ns backtype.storm.transactional-test
  (:use [clojure test])
  (:import [backtype.storm.topology TopologyBuilder])
  (:import [backtype.storm.transactional TransactionalSpoutCoordinator ITransactionalSpout
            ITransactionalSpout$Coordinator])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])  
  )

(bootstrap)

;; Testing TODO:
;; 
;; * Test that batch emitters emit nothing when a future batch has been emitted and its state saved - test batch emitter on its own?
;; * Test that transactionalbolts only commit when they've received the whole batch for that attempt,
;;   not a partial batch - test on its own
;; * Test that commit isn't considered successful until the entire tree has been completed (including tuples emitted from commit method)
;;      - test the full topology (this is a test of acking/anchoring)
;; * Test that batch isn't considered processed until the entire tuple tree has been completed
;;      - test the full topology (this is a test of acking/anchoring)
;; * Test that it picks up where it left off when restarting the topology
;;      - run topology and restart it
;; * Test that coordinator and partitioned state are cleaned up properly (and not too early) - test rotatingtransactionalstate


;; * Test that commits are strongly ordered - test coordinator on its own
;; * Test that commits are strongly ordered even in the case of failure - test coordinator on its own
;; * Test that it repeats the meta on a fail instead of recmoputing (for both partition state and coordinator state)
;; * Test that transactions are properly pipelined - test coordinator on its own
(defn mk-coordinator-state-changer [atom]
  (TransactionalSpoutCoordinator.
    (reify ITransactionalSpout
      (getComponentConfiguration [this]
        nil)
      (getCoordinator [this conf context]
        (reify ITransactionalSpout$Coordinator
          (initializeTransaction [this txid prevMetadata]
            @atom )
          (close [this]
            )))
    )))

(defn mk-spout-capture [capturer]
  (SpoutOutputCollector.
    (reify ISpoutOutputCollector
      (emit [this stream-id tuple message-id]
        (swap! capturer update-in [stream-id]
          (fn [oldval] (concat oldval [{:tuple tuple :id message-id}])))
        []
        ))))

(deftest test-coordinator
  (let [zk-port (available-port 2181)
        coordinator-state (atom nil)
        emit-capture (atom nil)]
    (with-inprocess-zookeeper zk-port
      (letlocals
        (bind coordinator
              (mk-coordinator-state-changer coordinator-state))
        (.open coordinator
               (merge (read-default-config)
                       {TOPOLOGY-MAX-SPOUT-PENDING 4
                       TOPOLOGY-TRANSACTIONAL-ID "abc"
                       STORM-ZOOKEEPER-PORT 2181
                       STORM-ZOOKEEPER-SERVERS ["localhost"]
                       })
               nil
               (mk-spout-capture emit-capture))
        (reset! coordinator-state 10)
        (.nextTuple coordinator)
        ;; check that there are 4 separate transaction attempts in there, with different ids
        ;; now fail the second one
        ;; check that it gets restarted
        ;; ack the second transaction id
        ;; check that no commit
        ;; ack the first one
        ;; check commit
        ;; ack the commit
        ;; check that the second one commits and a new batch added
        ;; ack the third one
        ;; check commit
        ;; ack the 4th one
        ;; check no commit on 4th
        ;; fail the commit
        ;; check that batch is retried and no commit on 4th
        ;; ack the 3rd + ack the commit
        ;; check that 4th is committed
        (println @emit-capture)
        ))))