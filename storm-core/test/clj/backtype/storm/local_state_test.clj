(ns backtype.storm.local-state-test
  (:use [clojure test])
  (:use [backtype.storm testing])
  (:import [backtype.storm.utils LocalState]))

(deftest test-local-state
  (with-local-tmp [dir1 dir2]
    (let [ls1 (LocalState. dir1)
          ls2 (LocalState. dir2)]
      (is (= {} (.snapshot ls1)))
      (.put ls1 "a" 1)
      (.put ls1 "b" 2)
      (is (= {"a" 1 "b" 2} (.snapshot ls1)))
      (is (= {} (.snapshot ls2)))
      (is (= 1 (.get ls1 "a")))
      (is (= nil (.get ls1 "c")))
      (is (= 2 (.get ls1 "b")))
      (is (= {"a" 1 "b" 2} (.snapshot (LocalState. dir1))))
      (.put ls2 "b" 1)
      (.put ls2 "b" 2)
      (.put ls2 "b" 3)
      (.put ls2 "b" 4)
      (.put ls2 "b" 5)
      (.put ls2 "b" 6)
      (.put ls2 "b" 7)
      (.put ls2 "b" 8)
      (is (= 8 (.get ls2 "b")))
      )))
