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
(ns org.apache.storm.command.healthcheck
  (:require [org.apache.storm
             [config :refer :all]
             [log :refer :all]]
            [clojure.java [io :as io]]
            [clojure [string :refer [split]]])
  (:gen-class))

(defn interrupter
  "Interrupt a given thread after ms milliseconds."
  [thread ms]
  (let [interrupter (Thread.
                     (fn []
                       (try
                         (Thread/sleep ms)
                         (.interrupt thread)
                         (catch InterruptedException e))))]
    (.start interrupter)
    interrupter))

(defn check-output [lines]
  (if (some #(.startsWith % "ERROR") lines)
    :failed
    :success))

(defn process-script [conf script]
  (let [script-proc (. (Runtime/getRuntime) (exec script))
        curthread (Thread/currentThread)
        interrupter-thread (interrupter curthread
                                        (conf STORM-HEALTH-CHECK-TIMEOUT-MS))]
    (try
      (.waitFor script-proc)
      (.interrupt interrupter-thread)
      (if (not (= (.exitValue script-proc) 0))
        :failed_with_exit_code
        (check-output (split
                       (slurp (.getInputStream script-proc))
                       #"\n+")))
      (catch InterruptedException e
        (println "Script" script "timed out.")
        :timeout)
      (catch Exception e
        (println "Script failed with exception: " e)
        :failed_with_exception)
      (finally (.interrupt interrupter-thread)))))

(defn health-check [conf]
  (let [health-dir (absolute-healthcheck-dir conf)
        health-files (file-seq (io/file health-dir))
        health-scripts (filter #(and (.canExecute %)
                                     (not (.isDirectory %)))
                               health-files)
        results (->> health-scripts
                     (map #(.getAbsolutePath %))
                     (map (partial process-script conf)))]
    (log-message
     (pr-str (map #'vector
                  (map #(.getAbsolutePath %) health-scripts)
                  results)))
    ; failed_with_exit_code is OK. We're mimicing Hadoop's health checks.
    ; We treat non-zero exit codes as indicators that the scripts failed
    ; to execute properly, not that the system is unhealthy, in which case
    ; we don't want to start killing things.
    (if (every? #(or (= % :failed_with_exit_code)
                     (= % :success))
                results)
      0
      1)))

(defn -main [& args]
  (let [conf (read-storm-config)]
    (System/exit
     (health-check conf))))
