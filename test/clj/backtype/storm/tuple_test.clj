(ns backtype.storm.tuple-test
  (:use [clojure test])
  (:import [backtype.storm.tuple Tuple])
  (:use [backtype.storm testing]))

(deftest test-lookup
  (let [ tuple (fake-tuple ["foo" "bar"] [12 "hello"]) ]
    (is (= 12 (tuple "foo")))
    (is (= 12 (tuple :foo)))
    (is (= 12 (:foo tuple)))

    (is (= "hello" (:bar tuple)))
    
    (is (= :notfound (tuple "404" :notfound)))))

(deftest test-indexed
  (let [ tuple (fake-tuple ["foo" "bar"] [12 "hello"]) ]
    (is (= 12 (nth tuple 0)))
    (is (= "hello" (nth tuple 1)))))

(deftest test-seq
  (let [ tuple (fake-tuple ["foo" "bar"] [12 "hello"]) ]
    (is (= [["foo" 12] ["bar" "hello"]] (seq tuple)))))

(deftest test-map
    (let [tuple (fake-tuple ["foo" "bar"] [12 "hello"]) ]
      (is (= {"foo" 42 "bar" "hello"} (.getMap (assoc tuple "foo" 42))))
      (is (= {"foo" 42 "bar" "hello"} (.getMap (assoc tuple :foo 42))))

      (is (= {"bar" "hello"} (.getMap (dissoc tuple "foo"))))
      (is (= {"bar" "hello"} (.getMap (dissoc tuple :foo))))

      (is (= {"foo" 42 "bar" "world"} (.getMap (assoc 
                                        (assoc tuple "foo" 42)
                                        :bar "world"))))))

