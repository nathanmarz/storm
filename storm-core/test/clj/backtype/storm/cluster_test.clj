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
(ns backtype.storm.cluster-test
  (:import [java.util Arrays])
  (:import [backtype.storm.daemon.common Assignment StormBase SupervisorInfo])
  (:use [clojure test])
  (:use [backtype.storm cluster config util testing]))

(defn mk-config [zk-port]
  (merge (read-storm-config)
         {STORM-ZOOKEEPER-PORT zk-port
          STORM-ZOOKEEPER-SERVERS ["localhost"]}))

(defn mk-state
  ([zk-port] (mk-distributed-cluster-state (mk-config zk-port)))
  ([zk-port cb]
     (let [ret (mk-state zk-port)]
       (.register ret cb)
       ret )))

(defn mk-storm-state [zk-port] (mk-storm-cluster-state (mk-config zk-port)))

(deftest test-basics
  (with-inprocess-zookeeper zk-port
    (let [state (mk-state zk-port)]
      (.set-data state "/root" (barr 1 2 3))
      (is (Arrays/equals (barr 1 2 3) (.get-data state "/root" false)))
      (is (= nil (.get-data state "/a" false)))
      (.set-data state "/root/a" (barr 1 2))
      (.set-data state "/root" (barr 1))
      (is (Arrays/equals (barr 1) (.get-data state "/root" false)))
      (is (Arrays/equals (barr 1 2) (.get-data state "/root/a" false)))
      (.set-data state "/a/b/c/d" (barr 99))
      (is (Arrays/equals (barr 99) (.get-data state "/a/b/c/d" false)))
      (.mkdirs state "/lalala")
      (is (= [] (.get-children state "/lalala" false)))
      (is (= #{"root" "a" "lalala"} (set (.get-children state "/" false))))
      (.delete-node state "/a")
      (is (= #{"root" "lalala"} (set (.get-children state "/" false))))
      (is (= nil (.get-data state "/a/b/c/d" false)))
      (.close state)
      )))

(deftest test-multi-state
  (with-inprocess-zookeeper zk-port
    (let [state1 (mk-state zk-port)
          state2 (mk-state zk-port)]
      (.set-data state1 "/root" (barr 1))
      (is (Arrays/equals (barr 1) (.get-data state1 "/root" false)))
      (is (Arrays/equals (barr 1) (.get-data state2 "/root" false)))
      (.delete-node state2 "/root")
      (is (= nil (.get-data state1 "/root" false)))
      (is (= nil (.get-data state2 "/root" false)))
      (.close state1)
      (.close state2)
      )))

(deftest test-ephemeral
  (with-inprocess-zookeeper zk-port
    (let [state1 (mk-state zk-port)
          state2 (mk-state zk-port)
          state3 (mk-state zk-port)]
      (.set-ephemeral-node state1 "/a" (barr 1))
      (is (Arrays/equals (barr 1) (.get-data state1 "/a" false)))
      (is (Arrays/equals (barr 1) (.get-data state2 "/a" false)))
      (.close state3)
      (is (Arrays/equals (barr 1) (.get-data state1 "/a" false)))
      (is (Arrays/equals (barr 1) (.get-data state2 "/a" false)))
      (.close state1)
      (is (= nil (.get-data state2 "/a" false)))
      (.close state2)
      )))

(defn mk-callback-tester []
  (let [last (atom nil)
        cb (fn [type path]
              (reset! last {:type type :path path}))]
    [last cb]
    ))

(defn read-and-reset! [aatom]
  (let [time (System/currentTimeMillis)]
  (loop []
    (if-let [val @aatom]
      (do
        (reset! aatom nil)
        val)
      (do
        (when (> (- (System/currentTimeMillis) time) 30000)
          (throw (RuntimeException. "Waited too long for atom to change state")))
        (Thread/sleep 10)
        (recur))
      ))))

(deftest test-callbacks
  (with-inprocess-zookeeper zk-port
    (let [[state1-last-cb state1-cb] (mk-callback-tester)
          state1 (mk-state zk-port state1-cb)
          [state2-last-cb state2-cb] (mk-callback-tester)
          state2 (mk-state zk-port state2-cb)]
      (.set-data state1 "/root" (barr 1))
      (.get-data state2 "/root" true)
      (is (= nil @state1-last-cb))
      (is (= nil @state2-last-cb))
      (.set-data state2 "/root" (barr 2))
      (is (= {:type :node-data-changed :path "/root"} (read-and-reset! state2-last-cb)))
      (is (= nil @state1-last-cb))

      (.set-data state2 "/root" (barr 3))
      (is (= nil @state2-last-cb))
      (.get-data state2 "/root" true)
      (.get-data state2 "/root" false)
      (.delete-node state1 "/root")
      (is (= {:type :node-deleted :path "/root"} (read-and-reset! state2-last-cb)))
      (.get-data state2 "/root" true)
      (.set-ephemeral-node state1 "/root" (barr 1 2 3 4))
      (is (= {:type :node-created :path "/root"} (read-and-reset! state2-last-cb)))

      (.get-children state1 "/" true)
      (.set-data state2 "/a" (barr 9))
      (is (= nil @state2-last-cb))
      (is (= {:type :node-children-changed :path "/"} (read-and-reset! state1-last-cb)))

      (.get-data state2 "/root" true)
      (.set-ephemeral-node state1 "/root" (barr 1 2))
      (is (= {:type :node-data-changed :path "/root"} (read-and-reset! state2-last-cb)))

      (.mkdirs state1 "/ccc")
      (.get-children state1 "/ccc" true)
      (.get-data state2 "/ccc/b" true)
      (.set-data state2 "/ccc/b" (barr 8))
      (is (= {:type :node-created :path "/ccc/b"} (read-and-reset! state2-last-cb)))
      (is (= {:type :node-children-changed :path "/ccc"} (read-and-reset! state1-last-cb)))

      (.get-data state2 "/root" true)
      (.get-data state2 "/root2" true)
      (.close state1)

      (is (= {:type :node-deleted :path "/root"} (read-and-reset! state2-last-cb)))
      (.set-data state2 "/root2" (barr 9))
      (is (= {:type :node-created :path "/root2"} (read-and-reset! state2-last-cb)))
      (.close state2)
      )))


(deftest test-storm-cluster-state-basics
  (with-inprocess-zookeeper zk-port
    (let [state (mk-storm-state zk-port)
          assignment1 (Assignment. "/aaa" {} {1 [2 2002 1]} {})
          assignment2 (Assignment. "/aaa" {} {1 [2 2002]} {})
          base1 (StormBase. "/tmp/storm1" 1 {:type :active} 2 {})
          base2 (StormBase. "/tmp/storm2" 2 {:type :active} 2 {})]
      (is (= [] (.assignments state nil)))
      (.set-assignment! state "storm1" assignment1)
      (is (= assignment1 (.assignment-info state "storm1" nil)))
      (is (= nil (.assignment-info state "storm3" nil)))
      (.set-assignment! state "storm1" assignment2)
      (.set-assignment! state "storm3" assignment1)
      (is (= #{"storm1" "storm3"} (set (.assignments state nil))))
      (is (= assignment2 (.assignment-info state "storm1" nil)))
      (is (= assignment1 (.assignment-info state "storm3" nil)))
      
      (is (= [] (.active-storms state)))
      (.activate-storm! state "storm1" base1)
      (is (= ["storm1"] (.active-storms state)))
      (is (= base1 (.storm-base state "storm1" nil)))
      (is (= nil (.storm-base state "storm2" nil)))
      (.activate-storm! state "storm2" base2)
      (is (= base1 (.storm-base state "storm1" nil)))
      (is (= base2 (.storm-base state "storm2" nil)))
      (is (= #{"storm1" "storm2"} (set (.active-storms state))))
      (.remove-storm-base! state "storm1")
      (is (= base2 (.storm-base state "storm2" nil)))
      (is (= #{"storm2"} (set (.active-storms state))))


      ;; TODO add tests for task info and task heartbeat setting and getting
      (.disconnect state)
      )))

(defn- validate-errors! [state storm-id component errors-list]
  (let [errors (.errors state storm-id component)]
    ;;(println errors)
    (is (= (count errors) (count errors-list)))
    (doseq [[error target] (map vector errors errors-list)]
      (when-not  (.contains (:error error) target)
        (println target " => " (:error error)))
      (is (.contains (:error error) target))
      )))


(deftest test-storm-cluster-state-errors
  (with-inprocess-zookeeper zk-port
    (with-simulated-time
      (let [state (mk-storm-state zk-port)]
        (.report-error state "a" "1"(local-hostname) 6700  (RuntimeException.))
        (validate-errors! state "a" "1" ["RuntimeException"])
        (advance-time-secs! 1)
        (.report-error state "a" "1" (local-hostname) 6700 (IllegalArgumentException.))
        (validate-errors! state "a" "1" ["IllegalArgumentException" "RuntimeException"])
        (doseq [i (range 10)]
          (.report-error state "a" "2" (local-hostname) 6700 (RuntimeException.))
          (advance-time-secs! 2))
        (validate-errors! state "a" "2" (repeat 10 "RuntimeException"))
        (doseq [i (range 5)]
          (.report-error state "a" "2" (local-hostname) 6700 (IllegalArgumentException.))
          (advance-time-secs! 2))
        (validate-errors! state "a" "2" (concat (repeat 5 "IllegalArgumentException")
                                                (repeat 5 "RuntimeException")
                                                ))
        (.disconnect state)
        ))))


(deftest test-supervisor-state
  (with-inprocess-zookeeper zk-port
    (let [state1 (mk-storm-state zk-port)
          state2 (mk-storm-state zk-port)]
      (is (= [] (.supervisors state1 nil)))
      (.supervisor-heartbeat! state2 "2" {:a 1})
      (.supervisor-heartbeat! state1 "1" {})
      (is (= {:a 1} (.supervisor-info state1 "2")))
      (is (= {} (.supervisor-info state1 "1")))
      (is (= #{"1" "2"} (set (.supervisors state1 nil))))
      (is (= #{"1" "2"} (set (.supervisors state2 nil))))
      (.disconnect state2)
      (is (= #{"1"} (set (.supervisors state1 nil))))
      (.disconnect state1)
      )))

(deftest test-storm-state-callbacks
  ;; TODO finish
  )



