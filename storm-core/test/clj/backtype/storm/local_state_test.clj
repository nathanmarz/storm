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
(ns backtype.storm.local-state-test
  (:use [clojure test])
  (:use [backtype.storm testing])
  (:import [backtype.storm.utils LocalState]))

(deftest test-local-state
  (with-local-tmp [dir1 dir2]
    (let [ls1 (LocalState. dir1)
          ls2 (LocalState. dir2)]
      (is (= {} (.snapshot ls1)))
      (.put ls1 "a" 1)
      (.put ls1 "b" 2)
      (is (= {"a" 1 "b" 2} (.snapshot ls1)))
      (is (= {} (.snapshot ls2)))
      (is (= 1 (.get ls1 "a")))
      (is (= nil (.get ls1 "c")))
      (is (= 2 (.get ls1 "b")))
      (is (= {"a" 1 "b" 2} (.snapshot (LocalState. dir1))))
      (.put ls2 "b" 1)
      (.put ls2 "b" 2)
      (.put ls2 "b" 3)
      (.put ls2 "b" 4)
      (.put ls2 "b" 5)
      (.put ls2 "b" 6)
      (.put ls2 "b" 7)
      (.put ls2 "b" 8)
      (is (= 8 (.get ls2 "b")))
      )))
