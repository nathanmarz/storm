(ns backtype.storm.drpc-test
  (:use [clojure test])
  (:import [backtype.storm.drpc ReturnResults DRPCSpout
            LinearDRPCTopologyBuilder])
  (:import [backtype.storm.topology FailedException])
  (:import [backtype.storm.coordination Coordinatedbolth$FinishedCallback])
  (:import [backtype.storm LocalDRPC LocalCluster])
  (:import [backtype.storm.tuple Fields])
  (:import [backtype.storm.generated DRPCExecutionException])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm clojure])
  )

(bootstrap)

(defbolth exclamation-bolth ["result" "return-info"] [tuple collector]
  (emit-bolth! collector
              [(str (.getString tuple 0) "!!!") (.getValue tuple 1)]
              :anchor tuple)
  (ack! collector tuple)
  )

(deftest test-drpc-flow
  (let [drpc (LocalDRPC.)
        spout (DRPCSpout. "test" drpc)
        cluster (LocalCluster.)
        topology (topology
                  {"1" (spout-spec spout)}
                  {"2" (bolth-spec {"1" :shuffle}
                                exclamation-bolth)
                   "3" (bolth-spec {"2" :shuffle}
                                (ReturnResults.))})]
    (.submitTopology cluster "test" {} topology)

    (is (= "aaa!!!" (.execute drpc "test" "aaa")))
    (is (= "b!!!" (.execute drpc "test" "b")))
    (is (= "c!!!" (.execute drpc "test" "c")))
    
    
    (.shutdown cluster)
    (.shutdown drpc)
    ))

(defbolth exclamation-bolth-drpc ["id" "result"] [tuple collector]
  (emit-bolth! collector
              [(.getValue tuple 0) (str (.getString tuple 1) "!!!")]
              :anchor tuple)
  (ack! collector tuple)
  )

(deftest test-drpc-builder
  (let [drpc (LocalDRPC.)
        cluster (LocalCluster.)
        builder (LinearDRPCTopologyBuilder. "test")
        ]
    (.addbolth builder exclamation-bolth-drpc 3)
    (.submitTopology cluster
                     "builder-test"
                     {}
                     (.createLocalTopology builder drpc))
    (is (= "aaa!!!" (.execute drpc "test" "aaa")))
    (is (= "b!!!" (.execute drpc "test" "b")))
    (is (= "c!!!" (.execute drpc "test" "c")))  
    
    (.shutdown cluster)
    (.shutdown drpc)
    ))

(defn safe-inc [v]
  (if v (inc v) 1))

(defbolth partial-count ["request" "count"] {:prepare true}
  [conf context collector]
  (let [counts (atom {})]
    (bolth
     (execute [tuple]
              (let [id (.getValue tuple 0)]
                (swap! counts update-in [id] safe-inc)
                (ack! collector tuple)
                ))
     Coordinatedbolth$FinishedCallback
     (finishedId [this id]
                 (emit-bolth! collector [id (get @counts id 0)])
                 ))
    ))

(defn safe+ [v1 v2]
  (if v1 (+ v1 v2) v2))

(defbolth count-aggregator ["request" "total"] {:prepare true}
  [conf context collector]
  (let [counts (atom {})]
    (bolth
     (execute [tuple]
              (let [id (.getValue tuple 0)
                    count (.getValue tuple 1)]
                (swap! counts update-in [id] safe+ count)
                (ack! collector tuple)
                ))
     Coordinatedbolth$FinishedCallback
     (finishedId [this id]
                 (emit-bolth! collector [id (get @counts id 0)])
                 ))
    ))

(defbolth create-tuples ["request"] [tuple collector]
  (let [id (.getValue tuple 0)
        amt (Integer/parseInt (.getValue tuple 1))]
    (doseq [i (range (* amt amt))]
      (emit-bolth! collector [id] :anchor tuple))
    (ack! collector tuple)
    ))

(deftest test-drpc-coordination
  (let [drpc (LocalDRPC.)
        cluster (LocalCluster.)
        builder (LinearDRPCTopologyBuilder. "square")
        ]
    (.addbolth builder create-tuples 3)
    (doto (.addbolth builder partial-count 3)
      (.shuffleGrouping))
    (doto (.addbolth builder count-aggregator 3)
      (.fieldsGrouping (Fields. ["request"])))

    (.submitTopology cluster
                     "squared"
                     {}
                     (.createLocalTopology builder drpc))
    (is (= "4" (.execute drpc "square" "2")))
    (is (= "100" (.execute drpc "square" "10")))
    (is (= "1" (.execute drpc "square" "1")))
    (is (= "0" (.execute drpc "square" "0")))
    
    
    (.shutdown cluster)
    (.shutdown drpc)
    ))

(defbolth id-bolth ["request" "val"] [tuple collector]
  (emit-bolth! collector
              (.getValues tuple)
              :anchor tuple)
  (ack! collector tuple))

(defbolth emit-finish ["request" "result"] {:prepare true}
  [conf context collector]
  (bolth
   (execute [tuple]
            (ack! collector tuple)
            )
   Coordinatedbolth$FinishedCallback
   (finishedId [this id]
               (emit-bolth! collector [id "done"])
               )))

(deftest test-drpc-coordination-tricky
  (let [drpc (LocalDRPC.)
        cluster (LocalCluster.)
        builder (LinearDRPCTopologyBuilder. "tricky")
        ]
    (.addbolth builder id-bolth 3)
    (doto (.addbolth builder id-bolth 3)
      (.shuffleGrouping))
    (doto (.addbolth builder emit-finish 3)
      (.fieldsGrouping (Fields. ["request"])))

    (.submitTopology cluster
                     "tricky"
                     {}
                     (.createLocalTopology builder drpc))
    (is (= "done" (.execute drpc "tricky" "2")))
    (is (= "done" (.execute drpc "tricky" "3")))
    (is (= "done" (.execute drpc "tricky" "4")))
    (.shutdown cluster)
    (.shutdown drpc)
    ))

(defbolth fail-finish-bolth ["request" "result"] {:prepare true}
  [conf context collector]
  (bolth
   (execute [tuple]
            (ack! collector tuple))
   Coordinatedbolth$FinishedCallback
   (finishedId [this id]
               (throw (FailedException.))
               )))

(deftest test-drpc-fail-finish
  (let [drpc (LocalDRPC.)
        cluster (LocalCluster.)
        builder (LinearDRPCTopologyBuilder. "fail2")
        ]
    (.addbolth builder fail-finish-bolth 3)

    (.submitTopology cluster
                     "fail2"
                     {}
                     (.createLocalTopology builder drpc))
    
    (is (thrown? DRPCExecutionException (.execute drpc "fail2" "2")))

    (.shutdown cluster)
    (.shutdown drpc)
    ))
