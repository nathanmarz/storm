(ns backtype.storm.config-test
  (:import [backtype.storm.utils Utils])
  (:use [clojure test])
  (:use [backtype.storm config])
  )

(deftest test-validity
  (is (Utils/isValidConf {TOPOLOGY-DEBUG true "q" "asasdasd" "aaa" (Integer. "123") "bbb" (Long. "456") "eee" [1 2 (Integer. "3") (Long. "4")]}))
  (is (not (Utils/isValidConf {"qqq" (backtype.storm.utils.Utils.)})))
  )
