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
(ns org.apache.storm.pacemaker-state-factory-test
  (:require [clojure.test :refer :all]
            [conjure.core :refer :all])
  (:import [org.apache.storm.generated
            HBExecutionException HBNodes HBRecords
            HBServerMessageType HBMessage HBMessageData HBPulse]
           [org.apache.storm.cluster ClusterStateContext  PaceMakerStateStorageFactory PaceMakerStateStorage]
           [org.mockito Mockito Matchers])
(:import [org.mockito.exceptions.base MockitoAssertionError])
(:import [org.apache.storm.pacemaker PacemakerClient])
(:import [org.apache.storm.testing.staticmocking MockedPaceMakerStateStorageFactory]))

(defn- string-to-bytes [string]
  (byte-array (map int string)))

(defn- bytes-to-string [bytez]
  (apply str (map char bytez)))

(defn- make-send-capture [response]
  (let [captured (atom nil)]
    (proxy [PacemakerClient] []
      (send [m] (reset! captured m) response)
      (checkCaptured [] @captured))))

(defmacro with-mock-pacemaker-client-and-state [client state pacefactory mock response & body]
  `(let [~client (make-send-capture ~response)
         ~pacefactory (Mockito/mock PaceMakerStateStorageFactory)]

     (with-open [~mock (MockedPaceMakerStateStorageFactory. ~pacefactory)]
       (. (Mockito/when (.initZKstateImpl ~pacefactory (Mockito/any) (Mockito/any) (Mockito/anyList) (Mockito/any))) (thenReturn nil))
       (. (Mockito/when (.initMakeClientImpl ~pacefactory (Mockito/any))) (thenReturn ~client))
               (let [~state (PaceMakerStateStorage. (PaceMakerStateStorageFactory/initMakeClient nil)
                   (PaceMakerStateStorageFactory/initZKstate nil  nil nil nil))]
                 ~@body))))

(deftest pacemaker_state_set_worker_hb
  (testing "set_worker_hb"
    (with-mock-pacemaker-client-and-state
      client state pacefactory mock
      (HBMessage. HBServerMessageType/SEND_PULSE_RESPONSE nil)

      (.set_worker_hb state "/foo" (string-to-bytes "data") nil)
      (let [sent (.checkCaptured client)
            pulse (.get_pulse (.get_data sent))]
        (is (= (.get_type sent) HBServerMessageType/SEND_PULSE))
        (is (= (.get_id pulse) "/foo"))
        (is (= (bytes-to-string (.get_details pulse)) "data")))))

  (testing "set_worker_hb"
    (with-mock-pacemaker-client-and-state
      client state pacefactory mock
      (HBMessage. HBServerMessageType/SEND_PULSE nil)

      (is (thrown? RuntimeException
            (.set_worker_hb state "/foo" (string-to-bytes "data") nil))))))


(deftest pacemaker_state_delete_worker_hb
  (testing "delete_worker_hb"
    (with-mock-pacemaker-client-and-state
      client state pacefactory mock
      (HBMessage. HBServerMessageType/DELETE_PATH_RESPONSE nil)

      (.delete_worker_hb state "/foo/bar")
      (let [sent (.checkCaptured client)]
        (is (= (.get_type sent) HBServerMessageType/DELETE_PATH))
        (is (= (.get_path (.get_data sent)) "/foo/bar")))))

  (testing "delete_worker_hb"
    (with-mock-pacemaker-client-and-state
      client state pacefactory mock
      (HBMessage. HBServerMessageType/DELETE_PATH nil)

      (is (thrown? RuntimeException
            (.delete_worker_hb state "/foo/bar"))))))

(deftest pacemaker_state_get_worker_hb
  (testing "get_worker_hb"
    (with-mock-pacemaker-client-and-state
      client state pacefactory mock
      (HBMessage. HBServerMessageType/GET_PULSE_RESPONSE
        (HBMessageData/pulse
          (doto (HBPulse.)
            (.set_id "/foo")
            (.set_details (string-to-bytes "some data")))))

      (.get_worker_hb state "/foo" false)
      (let [sent (.checkCaptured client)]
        (is (= (.get_type sent) HBServerMessageType/GET_PULSE))
        (is (= (.get_path (.get_data sent)) "/foo")))))

  (testing "get_worker_hb - fail (bad response)"
    (with-mock-pacemaker-client-and-state
      client state pacefactory mock
      (HBMessage. HBServerMessageType/GET_PULSE nil)

      (is (thrown? RuntimeException
            (.get_worker_hb state "/foo" false)))))

  (testing "get_worker_hb - fail (bad data)"
    (with-mock-pacemaker-client-and-state
      client state pacefactory mock
      (HBMessage. HBServerMessageType/GET_PULSE_RESPONSE nil)

      (is (thrown? RuntimeException
            (.get_worker_hb state "/foo" false))))))

(deftest pacemaker_state_get_worker_hb_children
  (testing "get_worker_hb_children"
    (with-mock-pacemaker-client-and-state
      client state pacefactory mock
      (HBMessage. HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE
        (HBMessageData/nodes
          (HBNodes. [])))

      (.get_worker_hb_children state "/foo" false)
      (let [sent (.checkCaptured client)]
        (is (= (.get_type sent) HBServerMessageType/GET_ALL_NODES_FOR_PATH))
        (is (= (.get_path (.get_data sent)) "/foo")))))

  (testing "get_worker_hb_children - fail (bad response)"
    (with-mock-pacemaker-client-and-state
      client state pacefactory mock
      (HBMessage. HBServerMessageType/DELETE_PATH nil)

      (is (thrown? RuntimeException
            (.get_worker_hb_children state "/foo" false)))))

  (testing "get_worker_hb_children - fail (bad data)"
    (with-mock-pacemaker-client-and-state
      client state pacefactory mock
      (HBMessage. HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE nil)

      (is (thrown? RuntimeException
            (.get_worker_hb_children state "/foo" false))))))

