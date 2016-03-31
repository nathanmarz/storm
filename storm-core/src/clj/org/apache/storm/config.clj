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

(ns org.apache.storm.config
  (:import [java.io FileReader File IOException]
           [org.apache.storm.generated StormTopology])
  (:import [org.apache.storm Config])
  (:import [org.apache.storm.utils Utils LocalState ConfigUtils MutableInt])
  (:import [org.apache.storm.validation ConfigValidation])
  (:import [org.apache.commons.io FileUtils])
  (:require [clojure [string :as str]])
  (:use [org.apache.storm log util]))

(defn- clojure-config-name [name]
  (.replace (.toUpperCase name) "_" "-"))

; define clojure constants for every configuration parameter
(doseq [f (seq (.getFields Config))]
  (let [name (.getName f)
        new-name (clojure-config-name name)]
    (eval
      `(def ~(symbol new-name) (. Config ~(symbol name))))))

(def ALL-CONFIGS
  (dofor [f (seq (.getFields Config))]
         (.get f nil)))

;; TODO this function and its callings will be replace when nimbus and supervisor move to Java
(defn cluster-mode
  [conf & args]
  (keyword (conf STORM-CLUSTER-MODE)))
