;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns org.apache.storm.serialization.SerializationFactory-test
  (:import [org.apache.storm Config])
  (:import [org.apache.storm.security.serialization BlowfishTupleSerializer])
  (:import [org.apache.storm.serialization SerializationFactory])
  (:import [org.apache.storm.utils ListDelegate])
  (:use [org.apache.storm config])
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
        (String. "org.apache.storm.security.serialization.BlowfishTupleSerializer")
        serializer-class (Class/forName arbitrary-class-name)
        arbitrary-key "0123456789abcdef"
        conf (merge (read-default-config)
          {Config/TOPOLOGY_TUPLE_SERIALIZER arbitrary-class-name
           BlowfishTupleSerializer/SECRET_KEY arbitrary-key})
        kryo (SerializationFactory/getKryo conf)]
    (is (= serializer-class (.getClass (.getSerializer kryo ListDelegate))))
  )
)
