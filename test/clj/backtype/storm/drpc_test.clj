(ns backtype.storm.drpc-test
  (:use [clojure test])
  (:import [backtype.storm.drpc ReturnResults DRPCSpout])
  (:import [backtype.storm LocalDRPC LocalCluster])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm clojure])
  )

(bootstrap)

(defbolt exclamation-bolt ["result" "return-info"] [tuple collector]
  (emit-bolt! collector
              [(str (.getString tuple 0) "!!!") (.getValue tuple 1)]
              :anchor tuple)
  )

(deftest test-drpc-flow
  (let [drpc (LocalDRPC.)
        spout (DRPCSpout. "test" drpc)
        cluster (LocalCluster.)
        topology (topology
                  {1 (spout-spec spout)}
                  {2 (bolt-spec {1 :shuffle}
                                exclamation-bolt)
                   3 (bolt-spec {2 :shuffle}
                                (ReturnResults.))})]
    (.submitTopology cluster "test" {TOPOLOGY-DEBUG true} topology)

    (is (= "aaa!!!") (.execute drpc "test" "aaa"))
    (is (= "b!!!") (.execute drpc "test" "b"))
    (is (= "c!!!") (.execute drpc "test" "c"))
    
    
    (.shutdown cluster)
    (.shutdown drpc)
    ))
