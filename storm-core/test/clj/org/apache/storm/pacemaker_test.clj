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
(ns org.apache.storm.pacemaker-test
  (:require [clojure.test :refer :all]
            [org.apache.storm.pacemaker [pacemaker :as pacemaker]]
            [conjure.core :as conjure])
  (:import [org.apache.storm.generated
            HBExecutionException HBServerMessageType
            HBMessage HBMessageData HBPulse]))

(defn- message-with-rand-id [type data]
  (let [mid (rand-int 1000)
        message (HBMessage. type data)]
    (.set_message_id message mid)
    [message mid]))

(defn- string-to-bytes [string]
  (byte-array (map int string)))

(defn- bytes-to-string [bytez]
  (apply str (map char bytez)))

(defn- makenode [handler path]
  (.handleMessage handler
                  (HBMessage.
                   HBServerMessageType/SEND_PULSE
                   (HBMessageData/pulse
                    (doto (HBPulse.)
                      (.set_id path)
                      (.set_details (string-to-bytes "nothing")))))
                  true))

(deftest pacemaker-server-create-path
  (conjure/stubbing
   [pacemaker/register nil]
   (let [handler (pacemaker/mk-handler {})]
     (testing "CREATE_PATH"
       (let [[message mid] (message-with-rand-id
                            HBServerMessageType/CREATE_PATH
                            (HBMessageData/path "/testpath"))
             response (.handleMessage handler message true)]
         (is (= (.get_message_id response) mid))
         (is (= (.get_type response) HBServerMessageType/CREATE_PATH_RESPONSE))
         (is (= (.get_data response) nil)))))))

(deftest pacemaker-server-exists
  (conjure/stubbing
   [pacemaker/register nil]
   (let [handler (pacemaker/mk-handler {})]
     (testing "EXISTS - false"
       (let [[message mid] (message-with-rand-id HBServerMessageType/EXISTS
                                                 (HBMessageData/path "/testpath"))
             bad-response (.handleMessage handler message false)
             good-response (.handleMessage handler message true)]
         (is (= (.get_message_id bad-response) mid))
         (is (= (.get_type bad-response) HBServerMessageType/NOT_AUTHORIZED))

         (is (= (.get_message_id good-response) mid))
         (is (= (.get_type good-response) HBServerMessageType/EXISTS_RESPONSE))
         (is (= (.get_boolval (.get_data good-response)) false))))

     (testing "EXISTS - true"
       (let [path "/exists_path"
             data-string "pulse data"]
         (let [[send _] (message-with-rand-id
                         HBServerMessageType/SEND_PULSE
                         (HBMessageData/pulse
                          (doto (HBPulse.)
                            (.set_id path)
                            (.set_details (string-to-bytes data-string)))))
               _ (.handleMessage handler send true)
               [message mid] (message-with-rand-id HBServerMessageType/EXISTS
                                                   (HBMessageData/path path))
               bad-response (.handleMessage handler message false)
               good-response (.handleMessage handler message true)]
           (is (= (.get_message_id bad-response) mid))
           (is (= (.get_type bad-response) HBServerMessageType/NOT_AUTHORIZED))

           (is (= (.get_message_id good-response) mid))
           (is (= (.get_type good-response) HBServerMessageType/EXISTS_RESPONSE))
           (is (= (.get_boolval (.get_data good-response)) true))))))))

(deftest pacemaker-server-send-pulse-get-pulse
  (conjure/stubbing
   [pacemaker/register nil]
   (let [handler (pacemaker/mk-handler {})]
     (testing "SEND_PULSE - GET_PULSE"
       (let [path "/pulsepath"
             data-string "pulse data"]
         (let [[message mid] (message-with-rand-id
                              HBServerMessageType/SEND_PULSE
                              (HBMessageData/pulse
                               (doto (HBPulse.)
                                 (.set_id path)
                                 (.set_details (string-to-bytes data-string)))))
               response (.handleMessage handler message true)]
           (is (= (.get_message_id response) mid))
           (is (= (.get_type response) HBServerMessageType/SEND_PULSE_RESPONSE))
           (is (= (.get_data response) nil)))
         (let [[message mid] (message-with-rand-id
                              HBServerMessageType/GET_PULSE
                              (HBMessageData/path path))
               response (.handleMessage handler message true)]
           (is (= (.get_message_id response) mid))
           (is (= (.get_type response) HBServerMessageType/GET_PULSE_RESPONSE))
           (is (= (bytes-to-string (.get_details (.get_pulse (.get_data response)))) data-string))))))))

(deftest pacemaker-server-get-all-pulse-for-path
  (conjure/stubbing
   [pacemaker/register nil]
   (let [handler (pacemaker/mk-handler {})]
     (testing "GET_ALL_PULSE_FOR_PATH"
       (let [[message mid] (message-with-rand-id HBServerMessageType/GET_ALL_PULSE_FOR_PATH
                                                 (HBMessageData/path "/testpath"))
             bad-response (.handleMessage handler message false)
             good-response (.handleMessage handler message true)]
         (is (= (.get_message_id bad-response) mid))
         (is (= (.get_type bad-response) HBServerMessageType/NOT_AUTHORIZED))

         (is (= (.get_message_id good-response) mid))
         (is (= (.get_type good-response) HBServerMessageType/GET_ALL_PULSE_FOR_PATH_RESPONSE))
         (is (= (.get_data good-response) nil)))))))

(deftest pacemaker-server-get-all-nodes-for-path
  (conjure/stubbing
   [pacemaker/register nil]
   (let [handler (pacemaker/mk-handler {})]
     (testing "GET_ALL_NODES_FOR_PATH"
       (makenode handler "/some-root-path/foo")
       (makenode handler "/some-root-path/bar")
       (makenode handler "/some-root-path/baz")
       (makenode handler "/some-root-path/boo")
       (let [[message mid] (message-with-rand-id HBServerMessageType/GET_ALL_NODES_FOR_PATH
                                                 (HBMessageData/path "/some-root-path"))
             bad-response (.handleMessage handler message false)
             good-response (.handleMessage handler message true)
             ids (into #{} (.get_pulseIds (.get_nodes (.get_data good-response))))]
         (is (= (.get_message_id bad-response) mid))
         (is (= (.get_type bad-response) HBServerMessageType/NOT_AUTHORIZED))

         (is (= (.get_message_id good-response) mid))
         (is (= (.get_type good-response) HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE))
         (is (contains? ids "foo"))
         (is (contains? ids "bar"))
         (is (contains? ids "baz"))
         (is (contains? ids "boo")))

       (makenode handler "/some/deeper/path/foo")
       (makenode handler "/some/deeper/path/bar")
       (makenode handler "/some/deeper/path/baz")
       (let [[message mid] (message-with-rand-id HBServerMessageType/GET_ALL_NODES_FOR_PATH
                                                 (HBMessageData/path "/some/deeper/path"))
             bad-response (.handleMessage handler message false)
             good-response (.handleMessage handler message true)
             ids (into #{} (.get_pulseIds (.get_nodes (.get_data good-response))))]
         (is (= (.get_message_id bad-response) mid))
         (is (= (.get_type bad-response) HBServerMessageType/NOT_AUTHORIZED))

         (is (= (.get_message_id good-response) mid))
         (is (= (.get_type good-response) HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE))
         (is (contains? ids "foo"))
         (is (contains? ids "bar"))
         (is (contains? ids "baz")))))))

(deftest pacemaker-server-get-pulse
  (conjure/stubbing
   [pacemaker/register nil]
   (let [handler (pacemaker/mk-handler {})]
     (testing "GET_PULSE"
       (makenode handler "/some-root/GET_PULSE")
       (let [[message mid] (message-with-rand-id HBServerMessageType/GET_PULSE
                                                 (HBMessageData/path "/some-root/GET_PULSE"))
             bad-response (.handleMessage handler message false)
             good-response (.handleMessage handler message true)
             good-pulse (.get_pulse (.get_data good-response))]
         (is (= (.get_message_id bad-response) mid))
         (is (= (.get_type bad-response) HBServerMessageType/NOT_AUTHORIZED))
         (is (= (.get_data bad-response) nil))

         (is (= (.get_message_id good-response) mid))
         (is (= (.get_type good-response) HBServerMessageType/GET_PULSE_RESPONSE))
         (is (= (.get_id good-pulse) "/some-root/GET_PULSE"))
         (is (= (bytes-to-string (.get_details good-pulse)) "nothing")))))))

(deftest pacemaker-server-delete-path
  (conjure/stubbing
   [pacemaker/register nil]
   (let [handler (pacemaker/mk-handler {})]
     (testing "DELETE_PATH"
       (makenode handler "/some-root/DELETE_PATH/foo")
       (makenode handler "/some-root/DELETE_PATH/bar")
       (makenode handler "/some-root/DELETE_PATH/baz")
       (makenode handler "/some-root/DELETE_PATH/boo")
       (let [[message mid] (message-with-rand-id HBServerMessageType/DELETE_PATH
                                                 (HBMessageData/path "/some-root/DELETE_PATH"))
             response (.handleMessage handler message true)]
         (is (= (.get_message_id response) mid))
         (is (= (.get_type response) HBServerMessageType/DELETE_PATH_RESPONSE))
         (is (= (.get_data response) nil)))
       (let [[message mid] (message-with-rand-id HBServerMessageType/GET_ALL_NODES_FOR_PATH
                                                 (HBMessageData/path "/some-root/DELETE_PATH"))
             response (.handleMessage handler message true)
             ids (into #{} (.get_pulseIds (.get_nodes (.get_data response))))]
         (is (= (.get_message_id response) mid))
         (is (= (.get_type response) HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE))
         (is (empty? ids)))))))

(deftest pacemaker-server-delete-pulse-id
  (conjure/stubbing
   [pacemaker/register nil]
   (let [handler (pacemaker/mk-handler {})]
     (testing "DELETE_PULSE_ID"
       (makenode handler "/some-root/DELETE_PULSE_ID/foo")
       (makenode handler "/some-root/DELETE_PULSE_ID/bar")
       (makenode handler "/some-root/DELETE_PULSE_ID/baz")
       (makenode handler "/some-root/DELETE_PULSE_ID/boo")
       (let [[message mid] (message-with-rand-id HBServerMessageType/DELETE_PULSE_ID
                                                 (HBMessageData/path "/some-root/DELETE_PULSE_ID/foo"))
             response (.handleMessage handler message true)]
         (is (= (.get_message_id response) mid))
         (is (= (.get_type response) HBServerMessageType/DELETE_PULSE_ID_RESPONSE))
         (is (= (.get_data response) nil)))
       (let [[message mid] (message-with-rand-id HBServerMessageType/GET_ALL_NODES_FOR_PATH
                                                 (HBMessageData/path "/some-root/DELETE_PULSE_ID"))
             response (.handleMessage handler message true)
             ids (into #{} (.get_pulseIds (.get_nodes (.get_data response))))]
         (is (= (.get_message_id response) mid))
         (is (= (.get_type response) HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE))
         (is (not (contains? ids "foo"))))))))
