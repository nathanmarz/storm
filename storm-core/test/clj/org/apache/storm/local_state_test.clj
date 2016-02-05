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
(ns org.apache.storm.local-state-test
  (:use [clojure test])
  (:use [org.apache.storm testing])
  (:import [org.apache.storm.utils LocalState]
           [org.apache.storm.generated GlobalStreamId]
           [org.apache.commons.io FileUtils]
           [java.io File]))

(deftest test-local-state
  (with-local-tmp [dir1 dir2]
    (let [gs-a (GlobalStreamId. "a" "a")
          gs-b (GlobalStreamId. "b" "b")
          gs-c (GlobalStreamId. "c" "c")
          gs-d (GlobalStreamId. "d" "d")
          ls1 (LocalState. dir1)
          ls2 (LocalState. dir2)]
      (is (= {} (.snapshot ls1)))
      (.put ls1 "a" gs-a)
      (.put ls1 "b" gs-b)
      (is (= {"a" gs-a "b" gs-b} (.snapshot ls1)))
      (is (= {} (.snapshot ls2)))
      (is (= gs-a (.get ls1 "a")))
      (is (= nil (.get ls1 "c")))
      (is (= gs-b (.get ls1 "b")))
      (is (= {"a" gs-a "b" gs-b} (.snapshot (LocalState. dir1))))
      (.put ls2 "b" gs-a)
      (.put ls2 "b" gs-b)
      (.put ls2 "b" gs-c)
      (.put ls2 "b" gs-d)
      (is (= gs-d (.get ls2 "b"))))))

(deftest empty-state
  (with-local-tmp [dir]
    (let [ls (LocalState. dir)
          gs-a (GlobalStreamId. "a" "a")
          data (FileUtils/openOutputStream (File. dir "12345"))
          version (FileUtils/openOutputStream (File. dir "12345.version"))]
      (is (= nil (.get ls "c")))
      (.put ls "a" gs-a)
      (is (= gs-a (.get ls "a"))))))
