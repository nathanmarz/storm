(ns backtype.storm.config-test
  (:import [backtype.storm Config ConfigValidation])
  (:import [backtype.storm.scheduler TopologyDetails])
  (:import [backtype.storm.utils Utils])
  (:use [clojure test])
  (:use [backtype.storm config util])
  )

(deftest test-validity
  (is (Utils/isValidConf {TOPOLOGY-DEBUG true "q" "asasdasd" "aaa" (Integer. "123") "bbb" (Long. "456") "eee" [1 2 (Integer. "3") (Long. "4")]}))
  (is (not (Utils/isValidConf {"qqq" (backtype.storm.utils.Utils.)})))
  )

(deftest test-power-of-2-validator
  (let [validator ConfigValidation/PowerOf2Validator]
    (doseq [x [42.42 42 23423423423 -33 -32 -1 -0.00001 0 -0 "Forty-two"]]
      (is (thrown-cause? java.lang.IllegalArgumentException
        (.validateField validator "test" x))))

    (doseq [x [64 4294967296 1 nil]]
      (is (nil? (try 
                  (.validateField validator "test" x)
                  (catch Exception e e)))))))

(deftest test-list-validator
  (let [validator ConfigValidation/StringsValidator]
    (doseq [x [
               ["Forty-two" 42]
               [42]
               [true "false"]
               [nil]
               [nil "nil"]
              ]]
      (is (thrown-cause-with-msg?
            java.lang.IllegalArgumentException #"(?i).*each element.*"
        (.validateField validator "test" x))))

    (doseq [x ["not a list at all"]]
      (is (thrown-cause-with-msg?
            java.lang.IllegalArgumentException #"(?i).*must be an iterable.*"
        (.validateField validator "test" x))))

    (doseq [x [
               ["one" "two" "three"]
               [""]
               ["42" "64"]
               nil
              ]]
    (is (nil? (try 
                (.validateField validator "test" x)
                (catch Exception e e)))))))

(deftest test-topology-workers-is-number
  (let [validator (CONFIG-SCHEMA-MAP TOPOLOGY-WORKERS)]
    (.validateField validator "test" 42)
    ;; The float can be rounded down to an int.
    (.validateField validator "test" 3.14159)
    (is (thrown-cause? java.lang.IllegalArgumentException
      (.validateField validator "test" "42")))))

(deftest test-isolation-scheduler-machines-is-map
  (let [validator (CONFIG-SCHEMA-MAP ISOLATION-SCHEDULER-MACHINES)]
    (is (nil? (try 
                (.validateField validator "test" {}) 
                (catch Exception e e))))
    (is (nil? (try 
                (.validateField validator "test" {"host0" 1 "host1" 2}) 
                (catch Exception e e))))
    (is (thrown-cause? java.lang.IllegalArgumentException
      (.validateField validator "test" 42)))))
