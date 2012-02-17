(ns backtype.storm.topology-config-test
  (:import [backtype.storm StormSubmitter])
  (:import [backtype.storm.testing TestWordSpout])
  (:use [clojure test]))

(defn config-item-test [config-key config-value]
    (def has-error false)
    (try
      (StormSubmitter/validateStormConf {config-key config-value})
      (catch IllegalArgumentException e
        (def has-error true)))
    has-error)

(deftest topology-config-test []
    ; test string config
    (is (= (config-item-test "foo" "bar") false))
    ; test integer config
    (is (= (config-item-test "foo" 1) false))
    ; test double config
    (is (= (config-item-test "foo" 1.1) false))
    ; test nil config
    (is (= (config-item-test "foo" nil) false))
    ; test boolean config
    (is (= (config-item-test "foo" true) false))
    ; test list config
    (is (= (config-item-test "foo" '("bar" 1)) false))
    ; test nested json config
    (is (= (config-item-test "foo" {"bar" 10}) false))
    ; test java bean config
    (is (= (config-item-test "foo" (TestWordSpout.)) true))
)
