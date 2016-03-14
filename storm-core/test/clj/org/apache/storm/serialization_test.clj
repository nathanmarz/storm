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
(ns org.apache.storm.serialization-test
  (:use [clojure test])
  (:import [org.apache.storm.serialization  SerializationTest]))


;TODO: We have moved needed tests to SerializationTest.
;TODO: When we move to java totally, we can remove this test file entirely
(deftest test-clojure-serialization
  (let [serializationTest (SerializationTest.)]
    (.isRoundtrip serializationTest [:a])
    (.isRoundtrip serializationTest [["a" 1 2 :a] 2 "aaa"])
    (.isRoundtrip serializationTest [#{:a :b :c}])
    (.isRoundtrip serializationTest [#{:a :b} 1 2 ["a" 3 5 #{5 6}]])
    (.isRoundtrip serializationTest [{:a [1 2 #{:a :b 1}] :b 3}])))