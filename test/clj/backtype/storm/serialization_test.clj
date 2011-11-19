(ns backtype.storm.serialization-test
  (:use [clojure test])
  (:import [java.io ByteArrayOutputStream DataOutputStream
            ByteArrayInputStream DataInputStream])
  (:import [backtype.storm.serialization KryoTupleSerializer KryoTupleDeserializer])
  (:import [backtype.storm.testing TestSerObject])
  (:use [backtype.storm util config])
  )


;; (defn mk-conf [extra]
;;   (merge (read-default-config) extra))
;; 
;; (deftest test-java-serialization
;;   (letlocals
;;       
;;    ))
;; 
;; (defn serialize [vals]
;;   (let [serializer (ValuesSerializer. (mk-conf {}))
;;         bos (ByteArrayOutputStream.)
;;         os (DataOutputStream. bos)]
;;     (.serializeInto serializer vals os)
;;     (.toByteArray bos)
;;     ))
;; 
;; (defn deserialize [bytes]
;;   (let [deserializer (ValuesDeserializer. (mk-conf {}))
;;         bin (ByteArrayInputStream. bytes)
;;         in (DataInputStream. bin)]
;;     (.deserializeFrom deserializer in)
;;     ))
;; 
;; (defn roundtrip [vals]
;;   (deserialize (serialize vals)))
;; 
;; (defn mk-string [size]
;;   (let [builder (StringBuilder.)]
;;     (doseq [i (range size)]
;;       (.append builder "a"))
;;     (.toString builder)))
;; 
;; (defn is-roundtrip [vals]
;;   (is (= vals (roundtrip vals))))
;; 
;; (deftest test-string-serialization
;;   (is-roundtrip ["a" "bb" "cde"])
;;   (is-roundtrip [(mk-string (* 64 1024))])
;;   (is-roundtrip [(mk-string (* 1024 1024))])
;;   )
