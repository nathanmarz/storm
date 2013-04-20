(ns backtype.storm.multilang-test
  (:use [clojure test])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  )

(bootstrap)

;; (deftest test-multilang-fy
;;   (with-local-cluster [cluster :supervisors 4]
;;     (let [nimbus (:nimbus cluster)
;;           topology (thrift/mk-topology
;;                       {"1" (thrift/mk-spout-spec (TestWordSpout. false))}
;;                       {"2" (thrift/mk-shell-bolt-spec {"1" :shuffle} "fancy" "tester.fy" ["word"] :parallelism-hint 1)}
;;                       )]
;;       (submit-local-topology nimbus
;;                           "test"
;;                           {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 20 TOPOLOGY-MESSAGE-TIMEOUT-SECS 3 TOPOLOGY-DEBUG true}
;;                           topology)
;;       (Thread/sleep 10000)
;;       (.killTopology nimbus "test")
;;       (Thread/sleep 10000)
;;       )))

(deftest test-multilang-rb
  (with-local-cluster [cluster :supervisors 4]
    (let [nimbus (:nimbus cluster)
          topology (thrift/mk-topology
                    {"1" (thrift/mk-shell-spout-spec ["ruby" "tester_spout.rb"] ["word"])}
                    {"2" (thrift/mk-shell-bolt-spec {"1" :shuffle} "ruby" "tester_bolt.rb" ["word"] :parallelism-hint 1)})]
      (submit-local-topology nimbus
                             "test"
                             {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 20 TOPOLOGY-MESSAGE-TIMEOUT-SECS 3 TOPOLOGY-DEBUG true}
                             topology)
      (Thread/sleep 10000)
      (.killTopology nimbus "test")
      (Thread/sleep 10000))))


(deftest test-multilang-py
  (with-local-cluster [cluster :supervisors 4]
    (let [nimbus (:nimbus cluster)
          topology (thrift/mk-topology
                      {"1" (thrift/mk-shell-spout-spec ["python" "tester_spout.py"] ["word"])}
                      {"2" (thrift/mk-shell-bolt-spec {"1" :shuffle} ["python" "tester_bolt.py"] ["word"] :parallelism-hint 1)}
                      )]
      (submit-local-topology nimbus
                          "test"
                          {TOPOLOGY-OPTIMIZE false TOPOLOGY-WORKERS 20 TOPOLOGY-MESSAGE-TIMEOUT-SECS 3 TOPOLOGY-DEBUG true}
                          topology)
      (Thread/sleep 10000)
      (.killTopology nimbus "test")
      (Thread/sleep 10000)
      )))
