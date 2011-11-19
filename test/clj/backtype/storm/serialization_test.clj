(ns backtype.storm.serialization-test
  (:use [clojure test])
  (:import [java.io ByteArrayOutputStream DataOutputStream
            ByteArrayInputStream DataInputStream])
  (:import [backtype.storm.serialization KryoTupleSerializer KryoTupleDeserializer
            KryoValuesSerializer KryoValuesDeserializer])
  (:import [backtype.storm.testing TestSerObject])
  (:use [backtype.storm util config])
  )


(defn mk-conf [extra]
  (merge (read-default-config) extra))

(defn serialize [vals conf]
  (let [serializer (KryoValuesSerializer. (mk-conf conf))
        bos (ByteArrayOutputStream.)]
    (.serializeInto serializer vals bos)
    (.toByteArray bos)
    ))

(defn deserialize [bytes conf]
  (let [deserializer (KryoValuesDeserializer. (mk-conf conf))
        bin (ByteArrayInputStream. bytes)]
    (.deserializeFrom deserializer bin)
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
   (= [obj] (roundtrip [obj] {TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION true}))   
   ))

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
  )
