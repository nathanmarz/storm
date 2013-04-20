(ns storm.trident.state-test
  (:use [clojure test])
  (:require [backtype.storm [testing :as t]])
  (:import [storm.trident.operation.builtin Count])
  (:import [storm.trident.state CombinerValueUpdater])
  (:import [storm.trident.state.map TransactionalMap OpaqueMap])
  (:import [storm.trident.testing MemoryBackingMap])
  (:use [storm.trident testing])
  (:use [backtype.storm util]))

(defn single-get [map key]
  (-> map (.multiGet [[key]]) first))

(defn single-update [map key amt]
  (-> map (.multiUpdate [[key]] [(CombinerValueUpdater. (Count.) amt)]) first))

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

