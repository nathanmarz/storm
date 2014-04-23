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

