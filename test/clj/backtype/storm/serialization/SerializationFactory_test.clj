(ns backtype.storm.serialization.SerializationFactory-test
  (:import [backtype.storm Config])
  (:import [backtype.storm.security.serialization BlowfishTupleSerializer])
  (:import [backtype.storm.serialization SerializationFactory])
  (:import [backtype.storm.utils ListDelegate])
  (:use [backtype.storm config])
  (:use [clojure test])
)


(deftest test-registers-default-when-not-in-conf
  (let [conf (read-default-config)
        klass-name (get conf Config/TOPOLOGY_TUPLE_SERIALIZER)
        configured-class (Class/forName klass-name)
        kryo (SerializationFactory/getKryo conf)]
    (is (= configured-class (.getClass (.getSerializer kryo ListDelegate))))
  )
)

(deftest test-throws-runtimeexception-when-no-such-class
  (let [conf (merge (read-default-config)
          {Config/TOPOLOGY_TUPLE_SERIALIZER "null.this.class.does.not.exist"})]
    (is (thrown? RuntimeException
      (SerializationFactory/getKryo conf)))
  )
)

(deftest test-registeres-when-valid-class-name
  (let [arbitrary-class-name
        (String. "backtype.storm.security.serialization.BlowfishTupleSerializer")
        serializer-class (Class/forName arbitrary-class-name)
        arbitrary-key "0123456789abcdef"
        conf (merge (read-default-config)
          {Config/TOPOLOGY_TUPLE_SERIALIZER arbitrary-class-name
           BlowfishTupleSerializer/SECRET_KEY arbitrary-key})
        kryo (SerializationFactory/getKryo conf)]
    (is (= serializer-class (.getClass (.getSerializer kryo ListDelegate))))
  )
)
