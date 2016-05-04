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
(ns org.apache.storm.starter.clj.bolts-test
  (:require [clojure.test :refer :all]
            [org.apache.storm.starter.clj.word-count :refer [word-count split-sentence]]
            [org.apache.storm.starter.clj.exclamation :refer [exclamation-bolt]]
            [org.apache.storm.starter.clj.bolts :refer
             [rolling-count-bolt intermediate-rankings-bolt total-rankings-bolt]]
            [org.apache.storm [testing :refer :all]])
  (:import [org.apache.storm Constants]
           [org.apache.storm.task OutputCollector IOutputCollector]
           [org.apache.storm.starter.tools Rankable]
           [org.apache.storm.tuple Tuple]))

(defn execute-tuples [bolt tuples]
  (let [out (atom [])]
    (.prepare bolt {} nil (OutputCollector.
                           (reify IOutputCollector
                             (emit [_ _ _ tuple]
                               (swap! out conj tuple))
                             (ack [_ input]))))
    (if (vector? tuples)
       (doseq [t tuples]
         (.execute bolt t))
       (.execute bolt tuples))
    @out))

(defn- mock-tuple [m & {component :component stream-id :stream-id
                        :or {component "1" stream-id "1"}}]
  (reify
    Tuple
    (getSourceComponent [_]
      component)
    (getSourceStreamId [_]
      stream-id)
    (getString [this i]
      (nth (vals m) 0))
    (getValues [_]
      (vals m))
    clojure.lang.IPersistentMap
    (valAt [_ key]
      (get m key))
    (seq [_]
      (seq m))))

(def ^{:private true} tick-tuple
  (mock-tuple {}
              :component Constants/SYSTEM_COMPONENT_ID
              :stream-id Constants/SYSTEM_TICK_STREAM_ID))

(deftest test-split-sentence
  (testing "Bolt emits word per sentence"
    (let [tuples (execute-tuples
                  split-sentence
                  (mock-tuple {"sentence" "the cat jumped over the door"}))]
      (is (= [["the"] ["cat"] ["jumped"] ["over"] ["the"] ["door"]] tuples)))))

(deftest test-word-count
  (testing "Bolt emits new count"
    (let [tuples (execute-tuples word-count [(mock-tuple {"word" "the"})
                                             (mock-tuple {"word" "the"})
                                             (mock-tuple {"word" "cat"})])]
      (is (ms= [["the" 1] ["the" 2] ["cat" 1]] tuples)))))

(deftest test-exclamation-bolt
  (testing "Bolt emits word with exclamation marks"
    (let [tuples (execute-tuples exclamation-bolt (mock-tuple {"word" "nathan"}))]
      (is (= [["nathan!!!"]] tuples)))))

(deftest test-rolling-bolt
  (testing "Emits nothing if no object has been counted"
    (let [tuples (execute-tuples (rolling-count-bolt 9 3) tick-tuple)]
      (is (empty? tuples))))
  (testing "Emits something if object was counted"
    (let [tuples (execute-tuples (rolling-count-bolt 9 3)
                                 [(mock-tuple {"word" "nathan"}) tick-tuple])]
      (is (= [["nathan" 1 0]] tuples)))))

(deftest test-intermediate-rankings-bolt
  (testing "Emits rankings for tick tuple"
    (let [tuples (execute-tuples (intermediate-rankings-bolt 5 2) tick-tuple)]
      (is (seq tuples))))
  (testing "Emits nothing for normal tuple"
    (let [tuples (execute-tuples (intermediate-rankings-bolt 5 2)
                                 (mock-tuple {"obj" "nathan" "count" 1}))]
      (is (empty? tuples)))))

(defn- mock-rankable [object count]
  "Creates rankable with object and count"
  (reify Rankable
    (getCount [_] count)
    (getObject [_] object)))

(deftest test-total-rankings-bolt
  (testing "Emits rankings for tick tuple"
    (let [tuples (execute-tuples (total-rankings-bolt 5 2) tick-tuple)]
      (is (seq tuples))))
  (testing "Emits nothing for normal tuple"
    (let [tuples (execute-tuples (total-rankings-bolt 5 2)
                                 (mock-tuple {"rankings" (mock-rankable "nathan" 2)}))]
      (is (empty? tuples)))))
