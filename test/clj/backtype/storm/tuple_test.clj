(ns backtype.storm.tuple-test
  (:use [clojure test])
  (:import [backtype.storm.tuple Tuple])
  (:use [backtype.storm testing]))

(deftest test-lookup
  (let [ tuple (test-tuple [12 "hello"] :fields ["foo" "bar"]) ]
    (is (= 12 (tuple "foo")))
    (is (= 12 (tuple :foo)))
    (is (= 12 (:foo tuple)))

    (is (= "hello" (:bar tuple)))
    
    (is (= :notfound (tuple "404" :notfound)))))

(deftest test-indexed
  (let [ tuple (test-tuple [12 "hello"] :fields ["foo" "bar"]) ]
    (is (= 12 (nth tuple 0)))
    (is (= "hello" (nth tuple 1)))))

(deftest test-seq
  (let [ tuple (test-tuple [12 "hello"] :fields ["foo" "bar"]) ]
    (is (= [["foo" 12] ["bar" "hello"]] (seq tuple)))))

(deftest test-map
    (let [tuple (test-tuple [12 "hello"] :fields ["foo" "bar"]) ]
      (is (= {"foo" 42 "bar" "hello"} (.getMap (assoc tuple "foo" 42))))
      (is (= {"foo" 42 "bar" "hello"} (.getMap (assoc tuple :foo 42))))

      (is (= {"bar" "hello"} (.getMap (dissoc tuple "foo"))))
      (is (= {"bar" "hello"} (.getMap (dissoc tuple :foo))))

      (is (= {"foo" 42 "bar" "world"} (.getMap (assoc 
                                        (assoc tuple "foo" 42)
                                        :bar "world"))))))

