(ns backtype.storm.fields-test
  (:use [clojure test])
  (:import [backtype.storm.tuple Fields])
  (:import [java.util List])
  (:import [java.util Iterator]))

(deftest test-fields-constructor
  (testing "constructor"
    (testing "with (String... fields)"
      (is (instance? Fields (Fields. (into-array String '("foo" "bar")))))
      (is (thrown? IllegalArgumentException (Fields. (into-array String '("foo" "bar" "foo"))))))
    (testing "with (List<String> fields)"
      (is (instance? Fields (Fields. '("foo" "bar"))))
      (is (thrown? IllegalArgumentException (Fields. '("foo" "bar" "foo")))))))

(deftest test-fields-methods
  (let [fields (Fields. '("foo" "bar"))]
    (testing "method"
      (testing ".size"
        (is (= (.size fields) 2)))
      (testing ".get"
        (is (= (.get fields 0) "foo"))
        (is (= (.get fields 1) "bar"))
        (is (thrown? IndexOutOfBoundsException (.get fields 2))))
      (testing ".fieldIndex"
        (is (= (.fieldIndex fields "foo") 0))
        (is (= (.fieldIndex fields "bar") 1))
        (is (thrown? IllegalArgumentException (.fieldIndex fields "baz"))))
      (testing ".contains"
        (is (= (.contains fields "foo") true))
        (is (= (.contains fields "bar") true))
        (is (= (.contains fields "baz") false)))
      (testing ".toList"
        (is (instance? List (.toList fields)))
        (is (= (count (.toList fields)) 2))
        (is (not-any? false? (map = (.toList fields) '("foo" "bar")))))
      (testing ".iterator"
        (is (instance? Iterator (.iterator fields)))
        (is (= (count (iterator-seq (.iterator fields))) 2))
        (is (not-any? false? (map = (iterator-seq (.iterator fields)) '("foo" "bar")))))
      (testing ".select"
        (is (instance? List (.select fields (Fields. '("bar")) '("a" "b" "c"))))
        (is (= (.select fields (Fields. '("bar")) '("a" "b" "c")) '("b")))))))

