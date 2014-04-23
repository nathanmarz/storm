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
(ns storm.trident.state-test
  (:use [clojure test])
  (:require [backtype.storm [testing :as t]])
  (:import [storm.trident.operation.builtin Count])
  (:import [storm.trident.state OpaqueValue])
  (:import [storm.trident.state CombinerValueUpdater])
  (:import [storm.trident.state.map TransactionalMap OpaqueMap])
  (:import [storm.trident.testing MemoryBackingMap])
  (:use [storm.trident testing])
  (:use [backtype.storm util]))

(defn single-get [map key]
  (-> map (.multiGet [[key]]) first))

(defn single-update [map key amt]
  (-> map (.multiUpdate [[key]] [(CombinerValueUpdater. (Count.) amt)]) first))

(deftest test-opaque-value
  (let [opqval (OpaqueValue. 8 "v1" "v0")
        upval0 (.update opqval 8 "v2")
        upval1 (.update opqval 9 "v2")
        ]
    (is (= "v1" (.get opqval nil)))
    (is (= "v1" (.get opqval 100)))
    (is (= "v1" (.get opqval 9)))
    (is (= "v0" (.get opqval 8)))
    (let [has-exception (try
                          (.get opqval 7) false
                          (catch Exception e true))]
      (is (= true has-exception)))
    (is (= "v0" (.getPrev opqval)))
    (is (= "v1" (.getCurr opqval)))
    ;; update with current
    (is (= "v0" (.getPrev upval0)))
    (is (= "v2" (.getCurr upval0)))
    (not (identical? opqval upval0))
    ;; update
    (is (= "v1" (.getPrev upval1)))
    (is (= "v2" (.getCurr upval1)))
    (not (identical? opqval upval1))
    ))

(deftest test-opaque-map
  (let [map (OpaqueMap/build (MemoryBackingMap.))]
    (.beginCommit map 1)
    (is (= nil (single-get map "a")))
    ;; tests that intra-batch caching works
    (is (= 1 (single-update map "a" 1)))
    (is (= 3 (single-update map "a" 2)))
    (.commit map 1)
    (.beginCommit map 1)
    (is (= nil (single-get map "a")))
    (is (= 2 (single-update map "a" 2)))
    (.commit map 1)
    (.beginCommit map 2)
    (is (= 2 (single-get map "a")))
    (is (= 5 (single-update map "a" 3)))
    (is (= 6 (single-update map "a" 1)))
    (.commit map 2)
    ))

(deftest test-transactional-map
  (let [map (TransactionalMap/build (MemoryBackingMap.))]
    (.beginCommit map 1)
    (is (= nil (single-get map "a")))
    ;; tests that intra-batch caching works
    (is (= 1 (single-update map "a" 1)))
    (is (= 3 (single-update map "a" 2)))
    (.commit map 1)
    (.beginCommit map 1)
    (is (= 3 (single-get map "a")))
    ;; tests that intra-batch caching has no effect if it's the same commit as previous commit
    (is (= 3 (single-update map "a" 1)))
    (is (= 3 (single-update map "a" 2)))
    (.commit map 1)
    (.beginCommit map 2)
    (is (= 3 (single-get map "a")))
    (is (= 6 (single-update map "a" 3)))
    (is (= 7 (single-update map "a" 1)))
    (.commit map 2)
    ))
