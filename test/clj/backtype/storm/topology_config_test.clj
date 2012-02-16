(ns backtype.storm.topology-config-test
  (:use [clojure test])
  (:import [backtype.storm StormSubmitter])
  (:use [backtype.storm bootstrap testing])
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm clojure]))

  (bootstrap) 


(defn config-item-test [config-key config-value]
  (def has-error false)
  (try
    (StormSubmitter/validateStormConf {config-key config-value})
    (catch IllegalArgumentException e
        (def has-error true)))
  has-error)

(deftest test-topology-config []
  (is (= (config-item-test TOPOLOGY-DEBUG 1) true))
  (is (= (config-item-test TOPOLOGY-DEBUG true) false))

  (is (= (config-item-test TOPOLOGY-OPTIMIZE 1) true))
  (is (= (config-item-test TOPOLOGY-OPTIMIZE true) false))

  (is (= (config-item-test TOPOLOGY-WORKERS true) true))
  (is (= (config-item-test TOPOLOGY-WORKERS 1) false))

  (is (= (config-item-test TOPOLOGY-ACKERS true) true))
  (is (= (config-item-test TOPOLOGY-ACKERS 1) false))

  (is (= (config-item-test TOPOLOGY-MESSAGE-TIMEOUT-SECS true) true))
  (is (= (config-item-test TOPOLOGY-MESSAGE-TIMEOUT-SECS 1) false))

  (is (= (config-item-test TOPOLOGY-KRYO-REGISTER 1) true))
  (is (= (config-item-test TOPOLOGY-KRYO-REGISTER '("foo", "bar")) false))

  (is (= (config-item-test TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS 1) true))
  (is (= (config-item-test TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS true) false))

  (is (= (config-item-test TOPOLOGY-MAX-TASK-PARALLELISM "foo") true))
  (is (= (config-item-test TOPOLOGY-MAX-TASK-PARALLELISM 1) false))

  (is (= (config-item-test TOPOLOGY-MAX-SPOUT-PENDING "foo") true))
  (is (= (config-item-test TOPOLOGY-MAX-SPOUT-PENDING 1) false))

  (is (= (config-item-test TOPOLOGY-STATE-SYNCHRONIZATION-TIMEOUT-SECS "foo") true))
  (is (= (config-item-test TOPOLOGY-STATE-SYNCHRONIZATION-TIMEOUT-SECS 1) false))

  (is (= (config-item-test TOPOLOGY-STATS-SAMPLE-RATE "foo") true))
  (is (= (config-item-test TOPOLOGY-STATS-SAMPLE-RATE 0.1) false))

  (is (= (config-item-test TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION "foo") true))
  (is (= (config-item-test TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION true) false))

  (is (= (config-item-test TOPOLOGY-WORKER-CHILDOPTS 1) true))
  (is (= (config-item-test TOPOLOGY-WORKER-CHILDOPTS "foo") false))

  (is (= (config-item-test TOPOLOGY-TRANSACTIONAL-ID 1) true))
  (is (= (config-item-test TOPOLOGY-TRANSACTIONAL-ID "foo") false))
)

