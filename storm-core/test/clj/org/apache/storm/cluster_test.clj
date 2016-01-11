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
(ns org.apache.storm.cluster-test
  (:import [java.util Arrays]
           [org.apache.storm.nimbus NimbusInfo])
  (:import [org.apache.storm.daemon.common Assignment StormBase SupervisorInfo])
  (:import [org.apache.storm.generated NimbusSummary])
  (:import [org.apache.zookeeper ZooDefs ZooDefs$Ids])
  (:import [org.mockito Mockito])
  (:import [org.mockito.exceptions.base MockitoAssertionError])
  (:import [org.apache.curator.framework CuratorFramework CuratorFrameworkFactory CuratorFrameworkFactory$Builder])
  (:import [org.apache.storm.utils Utils TestUtils ZookeeperAuthInfo])
  (:import [org.apache.storm.cluster ClusterState])
  (:require [org.apache.storm [zookeeper :as zk]])
  (:require [conjure.core])
  (:use [conjure core])
  (:use [clojure test])
  (:use [org.apache.storm cluster config util testing thrift log]))

(defn mk-config [zk-port]
  (merge (read-storm-config)
         {STORM-ZOOKEEPER-PORT zk-port
          STORM-ZOOKEEPER-SERVERS ["localhost"]}))

(defn mk-state
  ([zk-port] (let [conf (mk-config zk-port)]
               (mk-distributed-cluster-state conf :auth-conf conf)))
  ([zk-port cb]
     (let [ret (mk-state zk-port)]
       (.register ret cb)
       ret )))

(defn mk-storm-state [zk-port] (mk-storm-cluster-state (mk-config zk-port)))

(deftest test-basics
  (with-inprocess-zookeeper zk-port
    (let [state (mk-state zk-port)]
      (.set-data state "/root" (barr 1 2 3) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (Arrays/equals (barr 1 2 3) (.get-data state "/root" false)))
      (is (= nil (.get-data state "/a" false)))
      (.set-data state "/root/a" (barr 1 2) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (.set-data state "/root" (barr 1) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (Arrays/equals (barr 1) (.get-data state "/root" false)))
      (is (Arrays/equals (barr 1 2) (.get-data state "/root/a" false)))
      (.set-data state "/a/b/c/d" (barr 99) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (Arrays/equals (barr 99) (.get-data state "/a/b/c/d" false)))
      (.mkdirs state "/lalala" ZooDefs$Ids/OPEN_ACL_UNSAFE)
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
      (.set-data state1 "/root" (barr 1) ZooDefs$Ids/OPEN_ACL_UNSAFE)
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
      (.set-ephemeral-node state1 "/a" (barr 1) ZooDefs$Ids/OPEN_ACL_UNSAFE)
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
      (.set-data state1 "/root" (barr 1) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (.get-data state2 "/root" true)
      (is (= nil @state1-last-cb))
      (is (= nil @state2-last-cb))
      (.set-data state2 "/root" (barr 2) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (= {:type :node-data-changed :path "/root"} (read-and-reset! state2-last-cb)))
      (is (= nil @state1-last-cb))

      (.set-data state2 "/root" (barr 3) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (= nil @state2-last-cb))
      (.get-data state2 "/root" true)
      (.get-data state2 "/root" false)
      (.delete-node state1 "/root")
      (is (= {:type :node-deleted :path "/root"} (read-and-reset! state2-last-cb)))
      (.get-data state2 "/root" true)
      (.set-ephemeral-node state1 "/root" (barr 1 2 3 4) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (= {:type :node-created :path "/root"} (read-and-reset! state2-last-cb)))

      (.get-children state1 "/" true)
      (.set-data state2 "/a" (barr 9) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (= nil @state2-last-cb))
      (is (= {:type :node-children-changed :path "/"} (read-and-reset! state1-last-cb)))

      (.get-data state2 "/root" true)
      (.set-ephemeral-node state1 "/root" (barr 1 2) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (= {:type :node-data-changed :path "/root"} (read-and-reset! state2-last-cb)))

      (.mkdirs state1 "/ccc" ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (.get-children state1 "/ccc" true)
      (.get-data state2 "/ccc/b" true)
      (.set-data state2 "/ccc/b" (barr 8) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (= {:type :node-created :path "/ccc/b"} (read-and-reset! state2-last-cb)))
      (is (= {:type :node-children-changed :path "/ccc"} (read-and-reset! state1-last-cb)))

      (.get-data state2 "/root" true)
      (.get-data state2 "/root2" true)
      (.close state1)

      (is (= {:type :node-deleted :path "/root"} (read-and-reset! state2-last-cb)))
      (.set-data state2 "/root2" (barr 9) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (= {:type :node-created :path "/root2"} (read-and-reset! state2-last-cb)))
      (.close state2)
      )))


(deftest test-storm-cluster-state-basics
  (with-inprocess-zookeeper zk-port
    (let [state (mk-storm-state zk-port)
          assignment1 (Assignment. "/aaa" {} {[1] ["1" 1001 1]} {} {})
          assignment2 (Assignment. "/aaa" {} {[2] ["2" 2002]} {} {})
          nimbusInfo1 (NimbusInfo. "nimbus1" 6667 false)
          nimbusInfo2 (NimbusInfo. "nimbus2" 6667 false)
          nimbusSummary1 (NimbusSummary. "nimbus1" 6667 (current-time-secs) false "v1")
          nimbusSummary2 (NimbusSummary. "nimbus2" 6667 (current-time-secs) false "v2")
          base1 (StormBase. "/tmp/storm1" 1 {:type :active} 2 {} "" nil nil {})
          base2 (StormBase. "/tmp/storm2" 2 {:type :active} 2 {} "" nil nil {})]
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

      (is (nil? (.credentials state "storm1" nil)))
      (.set-credentials! state "storm1" {"a" "a"} {})
      (is (= {"a" "a"} (.credentials state "storm1" nil)))
      (.set-credentials! state "storm1" {"b" "b"} {})
      (is (= {"b" "b"} (.credentials state "storm1" nil)))

      (is (= [] (.blobstore-info state nil)))
      (.setup-blobstore! state "key1" nimbusInfo1 "1")
      (is (= ["key1"] (.blobstore-info state nil)))
      (is (= [(str (.toHostPortString nimbusInfo1) "-1")] (.blobstore-info state "key1")))
      (.setup-blobstore! state "key1" nimbusInfo2 "1")
      (is (= #{(str (.toHostPortString nimbusInfo1) "-1")
               (str (.toHostPortString nimbusInfo2) "-1")} (set (.blobstore-info state "key1"))))
      (.remove-blobstore-key! state "key1")
      (is (= [] (.blobstore-info state nil)))

      (is (= [] (.nimbuses state)))
      (.add-nimbus-host! state "nimbus1:port" nimbusSummary1)
      (is (= [nimbusSummary1] (.nimbuses state)))
      (.add-nimbus-host! state "nimbus2:port" nimbusSummary2)
      (is (= #{nimbusSummary1 nimbusSummary2} (set (.nimbuses state))))

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
        (.report-error state "a" "1" (local-hostname) 6700 (RuntimeException.))
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
          state2 (mk-storm-state zk-port)
          supervisor-info1 (SupervisorInfo. 10 "hostname-1" "id1" [1 2] [] {} 1000 "0.9.2" nil)
          supervisor-info2 (SupervisorInfo. 10 "hostname-2" "id2" [1 2] [] {} 1000 "0.9.2" nil)]
      (is (= [] (.supervisors state1 nil)))
      (.supervisor-heartbeat! state2 "2" supervisor-info2)
      (.supervisor-heartbeat! state1 "1" supervisor-info1)
      (is (= supervisor-info2 (.supervisor-info state1 "2")))
      (is (= supervisor-info1 (.supervisor-info state1 "1")))
      (is (= #{"1" "2"} (set (.supervisors state1 nil))))
      (is (= #{"1" "2"} (set (.supervisors state2 nil))))
      (.disconnect state2)
      (is (= #{"1"} (set (.supervisors state1 nil))))
      (.disconnect state1)
      )))

(deftest test-cluster-authentication
  (with-inprocess-zookeeper zk-port
    (let [builder (Mockito/mock CuratorFrameworkFactory$Builder)
          conf (merge
                (mk-config zk-port)
                {STORM-ZOOKEEPER-CONNECTION-TIMEOUT 10
                 STORM-ZOOKEEPER-SESSION-TIMEOUT 10
                 STORM-ZOOKEEPER-RETRY-INTERVAL 5
                 STORM-ZOOKEEPER-RETRY-TIMES 2
                 STORM-ZOOKEEPER-RETRY-INTERVAL-CEILING 15
                 STORM-ZOOKEEPER-AUTH-SCHEME "digest"
                 STORM-ZOOKEEPER-AUTH-PAYLOAD "storm:thisisapoorpassword"})]
      (. (Mockito/when (.connectString builder (Mockito/anyString))) (thenReturn builder))
      (. (Mockito/when (.connectionTimeoutMs builder (Mockito/anyInt))) (thenReturn builder))
      (. (Mockito/when (.sessionTimeoutMs builder (Mockito/anyInt))) (thenReturn builder))
      (TestUtils/testSetupBuilder builder (str zk-port "/") conf (ZookeeperAuthInfo. conf))
      (is (nil?
           (try
             (. (Mockito/verify builder) (authorization "digest" (.getBytes (conf STORM-ZOOKEEPER-AUTH-PAYLOAD))))
             (catch MockitoAssertionError e
               e)))))))

(deftest test-storm-state-callbacks
  ;; TODO finish
  )

(deftest test-cluster-state-default-acls
  (testing "The default ACLs are empty."
    (stubbing [zk/mkdirs nil
               zk/mk-client (reify CuratorFramework (^void close [this] nil))]
      (mk-distributed-cluster-state {})
      (verify-call-times-for zk/mkdirs 1)
      (verify-first-call-args-for-indices zk/mkdirs [2] nil))
    (stubbing [mk-distributed-cluster-state (reify ClusterState
                                              (register [this callback] nil)
                                              (mkdirs [this path acls] nil))]
      (mk-storm-cluster-state {})
      (verify-call-times-for mk-distributed-cluster-state 1)
      (verify-first-call-args-for-indices mk-distributed-cluster-state [4] nil))))
