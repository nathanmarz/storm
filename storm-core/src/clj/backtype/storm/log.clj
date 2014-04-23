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
(ns backtype.storm.log
  (:require [clojure.tools [logging :as log]]))

(defmacro log-message [& args]
  `(log/info (str ~@args)))

(defmacro log-error [e & args]
  `(log/log :error ~e (str ~@args)))

(defmacro log-debug [& args]
  `(log/debug (str ~@args)))

(defmacro log-warn-error [e & args]
  `(log/warn (str ~@args) ~e))

(defmacro log-warn [& args]
  `(log/warn (str ~@args)))

(defn log-capture! [& args]
  (apply log/log-capture! args))

(defn log-stream [& args]
  (apply log/log-stream args))
