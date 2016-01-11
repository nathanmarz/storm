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

(ns org.apache.storm.pacemaker.pacemaker
  (:import [org.apache.storm.pacemaker PacemakerServer IServerMessageHandler]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicInteger]
           [org.apache.storm.generated HBNodes
                                     HBServerMessageType HBMessage HBMessageData HBPulse]
           [org.apache.storm.utils VersionInfo])
  (:use [clojure.string :only [replace-first split]]
        [org.apache.storm log config util])
  (:require [clojure.java.jmx :as jmx])
  (:gen-class))

(def STORM-VERSION (VersionInfo/getVersion))

;; Stats Functions

(def sleep-seconds 60)


(defn- check-and-set-loop [stats key new & {:keys [compare new-fn]
                                            :or {compare (fn [new old] true)
                                                 new-fn (fn [new old] new)}}]
  (loop []
    (let [old (.get (key stats))
          new (new-fn new old)]
      (if (compare new old)
        (if (.compareAndSet (key stats) old new)
          nil
          (recur))
        nil))))

(defn- set-average [stats size]
  (check-and-set-loop
   stats
   :average-heartbeat-size
   size
   :new-fn (fn [new old]
            (let [count (.get (:send-pulse-count stats))]
                                        ; Weighted average
              (/ (+ new (* count old)) (+ count 1))))))

(defn- set-largest [stats size]
  (check-and-set-loop
   stats
   :largest-heartbeat-size
   size
   :compare #'>))

(defn- report-stats [heartbeats stats last-five-s]
  (loop []
    (let [send-count (.getAndSet (:send-pulse-count stats) 0)
          received-size (.getAndSet (:total-received-size stats) 0)
          get-count (.getAndSet (:get-pulse-count stats) 0)
          sent-size (.getAndSet (:total-sent-size stats) 0)
          largest (.getAndSet (:largest-heartbeat-size stats) 0)
          average (.getAndSet (:average-heartbeat-size stats) 0)
          total-keys (.size heartbeats)]
      (log-debug "\nReceived " send-count " heartbeats totaling " received-size " bytes,\n"
                 "Sent " get-count " heartbeats totaling " sent-size " bytes,\n"
                 "The largest heartbeat was " largest " bytes,\n"
                 "The average heartbeat was " average " bytes,\n"
                 "Pacemaker contained " total-keys " total keys\n"
                 "in the last " sleep-seconds " second(s)")
      (dosync (ref-set last-five-s
                       {:send-pulse-count send-count
                        :total-received-size received-size
                        :get-pulse-count get-count
                        :total-sent-size sent-size
                        :largest-heartbeat-size largest
                        :average-heartbeat-size average
                        :total-keys total-keys})))
    (Thread/sleep (* 1000 sleep-seconds))
    (recur)))

;; JMX stuff
(defn register [last-five-s]
  (jmx/register-mbean
    (jmx/create-bean
      last-five-s)
    "org.apache.storm.pacemaker.pacemaker:stats=Stats_Last_5_Seconds"))


;; Pacemaker Functions

(defn hb-data []
  (ConcurrentHashMap.))

(defn create-path [^String path heartbeats]
  (HBMessage. HBServerMessageType/CREATE_PATH_RESPONSE nil))

(defn exists [^String path heartbeats]
  (let [it-does (.containsKey heartbeats path)]
    (log-debug (str "Checking if path [" path "] exists..." it-does "."))
    (HBMessage. HBServerMessageType/EXISTS_RESPONSE
                (HBMessageData/boolval it-does))))

(defn send-pulse [^HBPulse pulse heartbeats pacemaker-stats]
  (let [id (.get_id pulse)
        details (.get_details pulse)]
    (log-debug (str "Saving Pulse for id [" id "] data [" + (str details) "]."))

    (.incrementAndGet (:send-pulse-count pacemaker-stats))
    (.addAndGet (:total-received-size pacemaker-stats) (alength details))
    (set-largest pacemaker-stats (alength details))
    (set-average pacemaker-stats (alength details))

    (.put heartbeats id details)
    (HBMessage. HBServerMessageType/SEND_PULSE_RESPONSE nil)))

(defn get-all-pulse-for-path [^String path heartbeats]
  (HBMessage. HBServerMessageType/GET_ALL_PULSE_FOR_PATH_RESPONSE nil))

(defn get-all-nodes-for-path [^String path ^ConcurrentHashMap heartbeats]
  (log-debug "List all nodes for path " path)
  (HBMessage. HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE
              (HBMessageData/nodes
               (HBNodes. (distinct (for [k (.keySet heartbeats)
                                         :let [trimmed-k (first
                                                          (filter #(not (= "" %))
                                                                  (split (replace-first k path "") #"/")))]
                                         :when (and
                                                (not (nil? trimmed-k))
                                                (= (.indexOf k path) 0))]
                                     trimmed-k))))))

(defn get-pulse [^String path heartbeats pacemaker-stats]
  (let [details (.get heartbeats path)]
    (log-debug (str "Getting Pulse for path [" path "]...data " (str details) "]."))


    (.incrementAndGet (:get-pulse-count pacemaker-stats))
    (if details
      (.addAndGet (:total-sent-size pacemaker-stats) (alength details)))

    (HBMessage. HBServerMessageType/GET_PULSE_RESPONSE
                (HBMessageData/pulse
                 (doto (HBPulse. ) (.set_id path) (.set_details details))))))

(defn delete-pulse-id [^String path heartbeats]
  (log-debug (str "Deleting Pulse for id [" path "]."))
  (.remove heartbeats path)
  (HBMessage. HBServerMessageType/DELETE_PULSE_ID_RESPONSE nil))

(defn delete-path [^String path heartbeats]
  (let [prefix (if (= \/ (last path)) path (str path "/"))]
    (doseq [k (.keySet heartbeats)
            :when (= (.indexOf k prefix) 0)]
      (delete-pulse-id k heartbeats)))
  (HBMessage. HBServerMessageType/DELETE_PATH_RESPONSE nil))

(defn not-authorized []
  (HBMessage. HBServerMessageType/NOT_AUTHORIZED nil))

(defn mk-handler [conf]
  (let [heartbeats ^ConcurrentHashMap (hb-data)
        pacemaker-stats {:send-pulse-count (AtomicInteger.)
                         :total-received-size (AtomicInteger.)
                         :get-pulse-count (AtomicInteger.)
                         :total-sent-size (AtomicInteger.)
                         :largest-heartbeat-size (AtomicInteger.)
                         :average-heartbeat-size (AtomicInteger.)}
        last-five (ref {:send-pulse-count 0
                        :total-received-size 0
                        :get-pulse-count 0
                        :total-sent-size 0
                        :largest-heartbeat-size 0
                        :average-heartbeat-size 0
                        :total-keys 0})
        stats-thread (Thread. (fn [] (report-stats heartbeats pacemaker-stats last-five)))]
    (.setDaemon stats-thread true)
    (.start stats-thread)
    (register last-five)
    (reify
      IServerMessageHandler
      (^HBMessage handleMessage [this ^HBMessage request ^boolean authenticated]
        (let [response
              (condp = (.get_type request)
                HBServerMessageType/CREATE_PATH
                (create-path (.get_path (.get_data request)) heartbeats)

                HBServerMessageType/EXISTS
                (if authenticated
                  (exists (.get_path (.get_data request)) heartbeats)
                  (not-authorized))

                HBServerMessageType/SEND_PULSE
                (send-pulse (.get_pulse (.get_data request)) heartbeats pacemaker-stats)

                HBServerMessageType/GET_ALL_PULSE_FOR_PATH
                (if authenticated
                  (get-all-pulse-for-path (.get_path (.get_data request)) heartbeats)
                  (not-authorized))

                HBServerMessageType/GET_ALL_NODES_FOR_PATH
                (if authenticated
                  (get-all-nodes-for-path (.get_path (.get_data request)) heartbeats)
                  (not-authorized))

                HBServerMessageType/GET_PULSE
                (if authenticated
                  (get-pulse (.get_path (.get_data request)) heartbeats pacemaker-stats)
                  (not-authorized))

                HBServerMessageType/DELETE_PATH
                (delete-path (.get_path (.get_data request)) heartbeats)

                HBServerMessageType/DELETE_PULSE_ID
                (delete-pulse-id (.get_path (.get_data request)) heartbeats)

                ; Otherwise
                (log-message "Got Unexpected Type: " (.get_type request)))]

          (.set_message_id response (.get_message_id request))
          response)))))

(defn launch-server! []
  (log-message "Starting pacemaker server for storm version '"
               STORM-VERSION
               "'")
  (let [conf (override-login-config-with-system-property (read-storm-config))]
    (PacemakerServer. (mk-handler conf) conf)))

(defn -main []
  (redirect-stdio-to-slf4j!)
  (launch-server!))
