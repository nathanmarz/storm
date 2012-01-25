(ns backtype.storm.transactional-test
  (:use [clojure test])
  (:import [backtype.storm.topology TopologyBuilder])
  (:import [backtype.storm.transactional TransactionalSpoutCoordinator ITransactionalSpout ITransactionalSpout$Coordinator TransactionAttempt
            TransactionalTopologyBuilder])
  (:import [backtype.storm.transactional.state TransactionalState RotatingTransactionalState RotatingTransactionalState$StateInitializer])
  (:import [backtype.storm.testing CountingBatchBolt MemoryTransactionalSpout])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])  
  )

(bootstrap)

;; Testing TODO:
;; 

;;  what about testing that the coordination is done properly?
;;  can check that it receives all the prior tuples before finishbatch is called in the full topology
;;  should test that it commits even when receiving no tuples (and test that finishBatch is called before commit in this case)


;; * Test that it repeats the meta for a partitioned state (test partitioned emitter on its own)
;; * Test that partitioned state emits nothing for the partition if it has seen a future transaction for that partition (test partitioned emitter on its own)




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

(def BATCH-STREAM TransactionalSpoutCoordinator/TRANSACTION_BATCH_STREAM_ID)
(def COMMIT-STREAM TransactionalSpoutCoordinator/TRANSACTION_COMMIT_STREAM_ID)

(defn mk-spout-capture [capturer]
  (SpoutOutputCollector.
    (reify ISpoutOutputCollector
      (emit [this stream-id tuple message-id]
        (swap! capturer update-in [stream-id]
          (fn [oldval] (concat oldval [{:tuple tuple :id message-id}])))
        []
        ))))

(defn verify-and-reset! [expected-map emitted-map-atom]
  (let [results @emitted-map-atom]
    (dorun
     (map-val
      (fn [tuples]
        (doseq [t tuples]
          (is (= (-> t :tuple first) (:id t)))
          ))
      results))
    (is (= expected-map
           (map-val
            (fn [tuples]
              (map (comp #(update % 0 (memfn getTransactionId))
                         vec
                         #(take 2 %)
                         :tuple)
                   tuples))
            results
            )))
    (reset! emitted-map-atom {})
    ))

(defn get-attempts [capture-atom stream]
  (map :id (get @capture-atom stream)))

(defn get-commit [capture-atom]
  (-> @capture-atom (get COMMIT-STREAM) first :id))

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
                       STORM-ZOOKEEPER-PORT zk-port
                       STORM-ZOOKEEPER-SERVERS ["localhost"]
                       })
               nil
               (mk-spout-capture emit-capture))
        (reset! coordinator-state 10)
        (.nextTuple coordinator)
        (bind attempts (get-attempts emit-capture BATCH-STREAM))
        (verify-and-reset! {BATCH-STREAM [[1 10] [2 10] [3 10] [4 10]]}
                           emit-capture)

        (.nextTuple coordinator)
        (verify-and-reset! {} emit-capture)
        
        (.fail coordinator (second attempts))
        (bind new-second-attempt (first (get-attempts emit-capture BATCH-STREAM)))
        (verify-and-reset! {BATCH-STREAM [[2 10]]} emit-capture)
        (is (not= new-second-attempt (second attempts)))
        (.ack coordinator new-second-attempt)
        (verify-and-reset! {} emit-capture)
        (.ack coordinator (first attempts))
        (bind commit-id (get-commit emit-capture))
        (verify-and-reset! {COMMIT-STREAM [[1]]} emit-capture)

        (reset! coordinator-state 12)
        (.ack coordinator commit-id)
        (bind commit-id (get-commit emit-capture))
        (verify-and-reset! {COMMIT-STREAM [[2]] BATCH-STREAM [[5 12]]} emit-capture)
        (.ack coordinator commit-id)
        (verify-and-reset! {BATCH-STREAM [[6 12]]} emit-capture)

        (.fail coordinator (nth attempts 2))
        (bind new-third-attempt (first (get-attempts emit-capture BATCH-STREAM)))
        (verify-and-reset! {BATCH-STREAM [[3 10]]} emit-capture)

        (.ack coordinator new-third-attempt)
        (bind commit-id (get-commit emit-capture))
        (verify-and-reset! {COMMIT-STREAM [[3]]} emit-capture)

        (.ack coordinator (nth attempts 3))
        (verify-and-reset! {} emit-capture)

        (.fail coordinator commit-id)
        (bind new-third-attempt (first (get-attempts emit-capture BATCH-STREAM)))
        (verify-and-reset! {BATCH-STREAM [[3 10]]} emit-capture)

        (.ack coordinator new-third-attempt)
        (bind commit-id (get-commit emit-capture))
        (verify-and-reset! {COMMIT-STREAM [[3]]} emit-capture)

        (.nextTuple coordinator)
        (verify-and-reset! {} emit-capture)
        
        (.ack coordinator commit-id)
        (verify-and-reset! {COMMIT-STREAM [[4]] BATCH-STREAM [[7 12]]} emit-capture)

        (.close coordinator)
        ))))

(defn verify-bolt-and-reset! [expected-map emitted-atom]
  (is (= expected-map @emitted-atom))
  (reset! emitted-atom {}))

(defn mk-bolt-capture [capturer]
  (let [adder (fn [amap key newvalue]
                (update-in
                 amap
                 [key]
                 (fn [ov]
                   (concat ov [newvalue])
                   )))]
    (DelegateOutputCollector.
     (reify IOutputCollector
       (emit [this stream-id anchors values]
         (swap! capturer adder stream-id values)
         []
         )
       (ack [this tuple]
         (swap! capturer adder :ack (.getValues tuple))
         )
       (fail [this tuple]
         (swap! capturer adder :fail (.getValues tuple)))
       ))))

(defn mk-attempt [txid attempt-id]
  (TransactionAttempt. (BigInteger. (str txid)) attempt-id))


(defn finish! [bolt id]
  (.finishedId bolt id))

(deftest test-batch-bolt
  (let [bolt (BatchBoltExecutor. (CountingBatchBolt.))
        capture-atom (atom {})
        attempt1-1 (mk-attempt 1 1)
        attempt1-2 (mk-attempt 1 2)
        attempt2-1 (mk-attempt 2 1)
        attempt3 (mk-attempt 3 1)
        attempt4 (mk-attempt 4 1)
        attempt5 (mk-attempt 5 1)
        attempt6 (mk-attempt 6 1)]
    (.prepare bolt {} nil (mk-bolt-capture capture-atom))


    ;; test that transactions are independent
    
    (.execute bolt (test-tuple [attempt1-1]))
    (.execute bolt (test-tuple [attempt1-1]))
    (.execute bolt (test-tuple [attempt1-2]))
    (.execute bolt (test-tuple [attempt2-1]))
    (.execute bolt (test-tuple [attempt1-1]))
    
    (finish! bolt attempt1-1)

    (verify-bolt-and-reset! {:ack [[attempt1-1] [attempt1-1] [attempt1-2]
                                   [attempt2-1] [attempt1-1]]
                             "batch" [[attempt1-1 3]]}
                            capture-atom)

    (.execute bolt (test-tuple [attempt1-2]))
    (finish! bolt attempt2-1)
    (verify-bolt-and-reset! {:ack [[attempt1-2]]
                             "batch" [[attempt2-1 1]]}
                            capture-atom)

    (finish! bolt attempt1-2)
    (verify-bolt-and-reset! {"batch" [[attempt1-2 2]]}
                            capture-atom)  
    ))

(defn mk-state-initializer [atom]
  (reify RotatingTransactionalState$StateInitializer
    (init [this txid last-state]
      @atom
      )))


(defn- to-txid [txid]
  (BigInteger. (str txid)))

(defn- get-state [state txid initializer]
  (.getState state (to-txid txid) initializer))

(defn- get-state-or-create [state txid initializer]
  (.getStateOrCreate state (to-txid txid) initializer))

(defn- cleanup-before [state txid]
  (.cleanupBefore state (to-txid txid)))

(deftest test-rotating-transactional-state
  ;; test strict ordered vs not strict ordered
  (let [zk-port (available-port 2181)]
    (with-inprocess-zookeeper zk-port
      (let [conf (merge (read-default-config)
                        {STORM-ZOOKEEPER-PORT zk-port
                         STORM-ZOOKEEPER-SERVERS ["localhost"]
                         })
            state (TransactionalState/newUserState conf "id1" {})
            strict-rotating (RotatingTransactionalState. state "strict" true)
            unstrict-rotating (RotatingTransactionalState. state "unstrict" false)
            init (atom 10)
            initializer (mk-state-initializer init)]
        (is (= 10 (get-state strict-rotating 1 initializer)))
        (is (= 10 (get-state strict-rotating 2 initializer)))
        (reset! init 20)
        (is (= 20 (get-state strict-rotating 3 initializer)))
        (is (= 10 (get-state strict-rotating 1 initializer)))

        (is (thrown? Exception (get-state strict-rotating 5 initializer)))
        (is (= 20 (get-state strict-rotating 4 initializer)))
        (is (= 4 (count (.list state "strict"))))
        (cleanup-before strict-rotating 3)
        (is (= 2 (count (.list state "strict"))))

        (is (nil? (get-state-or-create strict-rotating 5 initializer)))
        (is (= 20 (get-state-or-create strict-rotating 5 initializer)))
        (is (nil? (get-state-or-create strict-rotating 6 initializer)))        
        (cleanup-before strict-rotating 6)
        (is (= 1 (count (.list state "strict"))))

        (is (= 20 (get-state unstrict-rotating 10 initializer)))
        (is (= 20 (get-state unstrict-rotating 20 initializer)))
        (is (nil? (get-state unstrict-rotating 12 initializer)))
        (is (nil? (get-state unstrict-rotating 19 initializer)))
        (is (nil? (get-state unstrict-rotating 12 initializer)))
        (is (= 20 (get-state unstrict-rotating 21 initializer)))

        (.close state)
        ))))



;; * Test that commit isn't considered successful until the entire tree has been completed (including tuples emitted from commit method)
;;      - test the full topology (this is a test of acking/anchoring)
;; * Test that batch isn't considered processed until the entire tuple tree has been completed
;;      - test the full topology (this is a test of acking/anchoring)
;; * Test that it picks up where it left off when restarting the topology
;;      - run topology and restart it


(defn mk-transactional-source []
  (HashMap.))

(defn add-transactional-data [source partition-map]
  (doseq [[p data] partition-map]
    (if-not (contains? source p)
      (.put source p (Collections/synchronizedList (ArrayList.))))
    (-> source (.get p) (.addAll data))
    ))

;; cluster-map topology :mock-sources {} :storm-conf {}
(deftest test-transactional-topology
  (with-simulated-time-local-cluster [cluster]
    (letlocals
     (bind data (mk-transactional-source))
     (bind builder (TransactionalTopologyBuilder.
                    "id"
                    "spout"
                    (MemoryTransactionalSpout. data
                                               (Fields. ["word" "amt"])
                                               3)
                    2))


     (add-transactional-data data
                             {0 [["dog" 3]
                                 ["cat" 4]
                                 ["apple" 1]
                                 ["dog" 3]
                                 ["banana" 0]]
                              1 [["cat" 1]
                                 ["mango" 4]]
                              2 [["happy" 11]
                                 ["mango" 2]
                                 ["zebra" 1]]})
     (bind results
           (complete-topology cluster
                              (.buildTopology builder)
                              :storm-conf {TOPOLOGY-DEBUG true}))
     (println results)

     )))
