(ns backtype.storm.serialization-test
  (:use [clojure test])
  (:import [backtype.storm.serialization SerializationFactory JavaSerialization])
  (:import [backtype.storm.testing TestSerObject TestSerObjectSerialization])
  (:use [backtype.storm util config])
  )


(defn mk-conf [extra]
  (merge (read-default-config) extra))

(deftest test-java-serialization
  (letlocals
   (bind ser1 (SerializationFactory. (mk-conf {TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION true})))
   (bind ser2 (SerializationFactory. (mk-conf {TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION true
                                               TOPOLOGY-SERIALIZATIONS {33 (.getName TestSerObjectSerialization)}})))
   (bind ser3 (SerializationFactory. (mk-conf {TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false})))

   (bind fser (.getSerializationForClass ser1 TestSerObject))
   (is (= SerializationFactory/JAVA_SERIALIZATION_TOKEN (.getToken fser)))
   (is (= JavaSerialization (class (.getSerialization fser))))
   (is (= 33 (-> ser2 (.getSerializationForClass TestSerObject) .getToken)))
   (is (thrown? Exception (.getSerializationForClass ser3 TestSerObject)))

   (is (not-nil? (.getSerializationForToken ser1 SerializationFactory/JAVA_SERIALIZATION_TOKEN)))
   
   
   ))
