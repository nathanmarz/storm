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
(ns org.apache.storm.trident.state-test
  (:use [clojure test])
  (:require [org.apache.storm [testing :as t]])
  (:import [org.apache.storm.trident.operation.builtin Count])
  (:import [org.apache.storm.trident.state OpaqueValue])
  (:import [org.apache.storm.trident.state CombinerValueUpdater])
  (:import [org.apache.storm.trident.topology.state TransactionalState TestTransactionalState])
  (:import [org.apache.storm.trident.state.map TransactionalMap OpaqueMap])
  (:import [org.apache.storm.trident.testing MemoryBackingMap MemoryMapState])
  (:import [org.apache.storm.utils ZookeeperAuthInfo])
  (:import [org.apache.curator.framework CuratorFramework])
  (:import [org.apache.curator.framework.api CreateBuilder ProtectACLCreateModePathAndBytesable])
  (:import [org.apache.zookeeper CreateMode ZooDefs ZooDefs$Ids])
  (:import [org.mockito Matchers Mockito])
  (:import [org.mockito.exceptions.base MockitoAssertionError])
  (:use [org.apache.storm.trident testing])
  (:use [org.apache.storm config util]))

(defn single-remove [map key]
  (-> map (.multiRemove [[key]])))

(defn single-put [map key val]
  (-> map (.multiPut [[key]] [val])))

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
    (is (= 1 (single-get map "a")))
    (is (= 3 (single-update map "a" 2)))
    (is (= 3 (single-get map "a")))
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

(deftest test-create-node-acl
  (testing "Creates ZooKeeper nodes with the correct ACLs"
    (let [curator (Mockito/mock CuratorFramework)
          builder0 (Mockito/mock CreateBuilder)
          builder1 (Mockito/mock ProtectACLCreateModePathAndBytesable)
          expectedAcls ZooDefs$Ids/CREATOR_ALL_ACL]
      (. (Mockito/when (.create curator)) (thenReturn builder0))
      (. (Mockito/when (.creatingParentsIfNeeded builder0)) (thenReturn builder1))
      (. (Mockito/when (.withMode builder1 (Matchers/isA CreateMode))) (thenReturn builder1))
      (. (Mockito/when (.withACL builder1 (Mockito/anyList))) (thenReturn builder1))
      (TestTransactionalState/createNode curator "" (byte-array 0) expectedAcls nil)
      (is (nil?
        (try
          (. (Mockito/verify builder1) (withACL expectedAcls))
        (catch MockitoAssertionError e
          e)))))))

(deftest test-memory-map-state-remove
  (let [map (MemoryMapState. (uuid))]
    (.beginCommit map 1)
    (single-put map "a" 1)
    (single-put map "b" 2)
    (.commit map 1)
    (.beginCommit map 2)
    (single-remove map "a")
    (is (nil? (single-get map "a")))
    (is (= 2 (single-get map "b")))
    (.commit map 2)
    (.beginCommit map 2)
    (is (= 1 (single-get map "a")))
    (is (= 2 (single-get map "b")))
    (single-remove map "a")
    (.commit map 2)
    (.beginCommit map 3)
    (is (nil? (single-get map "a")))
    (is (= 2 (single-get map "b")))    
    (.commit map 3)
    ))
