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
(ns org.apache.storm.command.upload-credentials
  (:use [clojure.tools.cli :only [cli]])
  (:use [org.apache.storm log util])
  (:import [org.apache.storm StormSubmitter])
  (:import [java.util Properties])
  (:import [java.io FileReader])
  (:gen-class))

(defn read-map [file-name]
  (let [props (Properties. )
        _ (.load props (FileReader. file-name))]
    (clojurify-structure props)))

(defn -main [& args]
  (let [[{cred-file :file} [name & rawCreds]] (cli args ["-f" "--file" :default nil])
        _ (when (and rawCreds (not (even? (.size rawCreds)))) (throw (RuntimeException.  "Need an even number of arguments to make a map")))
        mapping (if rawCreds (apply assoc {} rawCreds) {})
        file-mapping (if (nil? cred-file) {} (read-map cred-file))]
      (StormSubmitter/pushCredentials name {} (merge file-mapping mapping))
      (log-message "Uploaded new creds to topology: " name)))
