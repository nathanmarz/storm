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

(ns backtype.storm.LocalDRPC
  (:require [backtype.storm.daemon [drpc :as drpc]])
  (:use [backtype.storm util])
  (:import [backtype.storm.utils InprocMessaging ServiceRegistry])
  (:gen-class
   :init init
   :implements [backtype.storm.ILocalDRPC]
   :constructors {[] []}
   :state state ))

(defn -init []
  (let [handler (drpc/service-handler)
        id (ServiceRegistry/registerService handler)
        ]
    [[] {:service-id id :handler handler}]
    ))

(defn -execute [this func funcArgs]
  (.execute (:handler (. this state)) func funcArgs)
  )

(defn -result [this id result]
  (.result (:handler (. this state)) id result)
  )

(defn -fetchRequest [this func]
  (.fetchRequest (:handler (. this state)) func)
  )

(defn -failRequest [this id]
  (.failRequest (:handler (. this state)) id)
  )

(defn -getServiceId [this]
  (:service-id (. this state)))

(defn -shutdown [this]
  (ServiceRegistry/unregisterService (:service-id (. this state)))
  (.shutdown (:handler (. this state)))
  )
