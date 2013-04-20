(ns backtype.storm.serialization-test
  (:use [clojure test])
  (:import [backtype.storm.serialization KryoTupleSerializer KryoTupleDeserializer
            KryoValuesSerializer KryoValuesDeserializer])
  (:import [backtype.storm.testing TestSerObject TestKryoDecorator])
  (:use [backtype.storm util config])
  )


(defn mk-conf [extra]
  (merge (read-default-config) extra))

(defn serialize [vals conf]
  (let [serializer (KryoValuesSerializer. (mk-conf conf))]
    (.serialize serializer vals)
    ))

(defn deserialize [bytes conf]
  (let [deserializer (KryoValuesDeserializer. (mk-conf conf))]
    (.deserialize deserializer bytes)
    ))

(defn roundtrip
  ([vals] (roundtrip vals {}))
  ([vals conf]
    (deserialize (serialize vals conf) conf)))

(deftest test-java-serialization
  (letlocals
   (bind obj (TestSerObject. 1 2))
   (is (thrown? Exception
     (roundtrip [obj] {TOPOLOGY-KRYO-REGISTER {"backtype.storm.testing.TestSerObject" nil}
                       TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false})))
   (is (= [obj] (roundtrip [obj] {TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION true})))))

(deftest test-kryo-decorator
  (letlocals
   (bind obj (TestSerObject. 1 2))
   (is (thrown? Exception
                (roundtrip [obj] {TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false})))
   
   (is (= [obj] (roundtrip [obj] {TOPOLOGY-KRYO-DECORATORS ["backtype.storm.testing.TestKryoDecorator"]
                                  TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false})))))

(defn mk-string [size]
  (let [builder (StringBuilder.)]
    (doseq [i (range size)]
      (.append builder "a"))
    (.toString builder)))

(defn is-roundtrip [vals]
  (is (= vals (roundtrip vals))))

(deftest test-string-serialization
  (is-roundtrip ["a" "bb" "cde"])
  (is-roundtrip [(mk-string (* 64 1024))])
  (is-roundtrip [(mk-string (* 1024 1024))])
  (is-roundtrip [(mk-string (* 1024 1024 2))])
  )

(deftest test-clojure-serialization
  (is-roundtrip [:a])
  (is-roundtrip [["a" 1 2 :a] 2 "aaa"])
  (is-roundtrip [#{:a :b :c}])
  (is-roundtrip [#{:a :b} 1 2 ["a" 3 5 #{5 6}]])
  (is-roundtrip [{:a [1 2 #{:a :b 1}] :b 3}]))
