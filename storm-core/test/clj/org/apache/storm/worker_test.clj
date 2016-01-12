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
(ns org.apache.storm.worker-test
  (:use [clojure test])
  (:require [org.apache.storm.daemon [worker :as worker]])
  (:require [org.apache.storm [util :as util]])
  (:require [conjure.core])
  (:require [clj-time.core :as time])
  (:require [clj-time.coerce :as coerce])
  (:import [org.apache.storm.generated LogConfig LogLevel LogLevelAction])
  (:import [org.apache.logging.log4j Level LogManager])
  (:import [org.slf4j Logger])
  (:use [conjure core])
  (:use [org.apache.storm testing log])
  (:use [org.apache.storm.daemon common])
  (:use [clojure.string :only [join]])
  (:import [org.apache.storm.messaging TaskMessage IContext IConnection ConnectionWithStatus ConnectionWithStatus$Status])
  (:import [org.mockito Mockito])
  )


(deftest test-log-reset-should-not-trigger-for-future-time
  (with-local-cluster [cluster]
    (let [worker (:worker cluster)
          present (time/now)
          the-future (coerce/to-long (time/plus present (time/secs 1)))
          mock-config {"foo" {:timeout the-future}}
          mock-config-atom (atom mock-config)]
      (stubbing [time/now present]
        (worker/reset-log-levels mock-config-atom)
        ;; if the worker doesn't reset log levels, the atom should not be nil
        (is (not(= @mock-config-atom nil)))))))

(deftest test-log-reset-triggers-for-past-time
  (with-local-cluster [cluster]
    (let [worker (:worker cluster)
          present (time/now)
          past (time/plus present (time/secs -1))
          mock-config {"foo" { :timeout (coerce/to-long past)
                               :target-log-level Level/INFO
                               :reset-log-level Level/WARN}}
          mock-config-atom (atom mock-config)]
      (stubbing [time/now present]
        (worker/reset-log-levels mock-config-atom)
        ;; the logger config is removed from atom
        (is (= @mock-config-atom {}))))))

(deftest test-log-reset-resets-does-nothing-for-empty-log-config
  (with-local-cluster [cluster]
    (let [worker (:worker cluster)
          present (time/now)
          past (coerce/to-long (time/plus present (time/secs -1)))
          mock-config {}
          mock-config-atom (atom mock-config)]
      (stubbing [worker/set-logger-level nil
                 time/now present]
        (worker/reset-log-levels mock-config-atom)
        ;; if the worker resets log level, the atom is nil'ed out
        (is (= @mock-config-atom {}))
        ;; test that the set-logger-level function was not called
        (verify-call-times-for worker/set-logger-level 0)))))

(deftest test-log-reset-resets-root-logger-if-set
  (with-local-cluster [cluster]
    (let [worker (:worker cluster)
          present (time/now)
          past (coerce/to-long (time/plus present (time/secs -1)))
          mock-config {LogManager/ROOT_LOGGER_NAME  {:timeout past
                                                     :target-log-level Level/DEBUG
                                                     :reset-log-level Level/WARN}}
          mock-config-atom (atom mock-config)]
      (stubbing [worker/set-logger-level nil
                 time/now present]
        (worker/reset-log-levels mock-config-atom)
        ;; if the worker resets log level, the atom is reset to {}
        (is (= @mock-config-atom {}))
        ;; ensure we reset back to WARN level
        (verify-call-times-for worker/set-logger-level 1)
        (verify-first-call-args-for-indices worker/set-logger-level [1 2] LogManager/ROOT_LOGGER_NAME Level/WARN)))))

;;This should be removed when it goes into conjure
(defmacro verify-nth-call-args-for-indices
  "Asserts that the function was called at least once, and the nth call was
   passed the args specified, into the indices of the arglist specified. In
   other words, it checks only the particular args you care about."
  [n fn-name indices & args]
  `(do
     (assert-in-fake-context "verify-first-call-args-for-indices")
     (assert-conjurified-fn "verify-first-call-args-for-indices" ~fn-name)
     (is (< ~n (count (get @call-times ~fn-name)))
         (str "(verify-nth-call-args-for-indices " ~n " " ~fn-name " " ~indices " " ~(join " " args) ")"))
     (let [nth-call-args# (nth (get @call-times ~fn-name) ~n)
           indices-in-range?# (< (apply max ~indices) (count nth-call-args#))]
       (if indices-in-range?#
         (is (= ~(vec args) (map #(nth nth-call-args# %) ~indices))
             (str "(verify-first-call-args-for-indices " ~n " " ~fn-name " " ~indices " " ~(join " " args) ")"))
         (is (= :fail (format "indices %s are out of range for the args, %s" ~indices ~(vec args)))
             (str "(verify-first-call-args-for-indices " ~n " " ~fn-name " " ~indices " " ~(join " " args) ")"))))))

(deftest test-log-resets-named-loggers-with-past-timeout
  (with-local-cluster [cluster]
    (let [worker (:worker cluster)
          present (time/now)
          past (coerce/to-long (time/plus present (time/secs -1)))
          mock-config {"my_debug_logger" {:timeout past
                                          :target-log-level Level/DEBUG
                                          :reset-log-level Level/INFO} 
                       "my_info_logger" {:timeout past
                                         :target-log-level Level/INFO
                                         :reset-log-level Level/WARN}
                       "my_error_logger" {:timeout past
                                          :target-log-level Level/ERROR
                                          :reset-log-level Level/INFO}}
          result (atom {})
          mock-config-atom (atom mock-config)]
      (stubbing [worker/set-logger-level nil
                 time/now present]
          (worker/reset-log-levels mock-config-atom)
          ;; if the worker resets log level, the atom is reset to {}
          (is (= @mock-config-atom {}))
          (verify-call-times-for worker/set-logger-level 3)
          (verify-nth-call-args-for-indices 0 worker/set-logger-level [1 2] "my_debug_logger" Level/INFO)
          (verify-nth-call-args-for-indices 1 worker/set-logger-level [1 2] "my_error_logger" Level/INFO)
          (verify-nth-call-args-for-indices 2 worker/set-logger-level [1 2] "my_info_logger" Level/WARN)))))

(deftest test-process-root-log-level-to-debug-sets-logger-and-timeout-2
  (with-local-cluster [cluster]
    (let [worker (:worker cluster)
          mock-config (LogConfig.)
          root-level (LogLevel.)
          mock-config-atom (atom nil)
          orig-levels (atom {})
          present (time/now)
          in-thirty-seconds (coerce/to-long (time/plus present (time/secs 30)))]
      ;; configure the root logger to be debug
      (.set_reset_log_level_timeout_epoch root-level in-thirty-seconds)
      (.set_target_log_level root-level "DEBUG")
      (.set_action root-level LogLevelAction/UPDATE)
      (.put_to_named_logger_level mock-config "ROOT" root-level)
      (stubbing [worker/set-logger-level nil
                 time/now present]
          (worker/process-log-config-change mock-config-atom orig-levels mock-config)
          ;; test that the set-logger-level function was not called
          (log-message "Tests " @mock-config-atom)
          (verify-call-times-for worker/set-logger-level 1)
          (verify-nth-call-args-for-indices 0 worker/set-logger-level [1 2] "" Level/DEBUG)
          (let [root-result (get @mock-config-atom LogManager/ROOT_LOGGER_NAME)]
            (is (= (:action root-result) LogLevelAction/UPDATE))
            (is (= (:target-log-level root-result) Level/DEBUG))
            ;; defaults to INFO level when the logger isn't found previously
            (is (= (:reset-log-level root-result) Level/INFO))
            (is (= (:timeout root-result) in-thirty-seconds)))))))

(deftest test-process-root-log-level-to-debug-sets-logger-and-timeout
  (with-local-cluster [cluster]
    (let [worker (:worker cluster)
          mock-config (LogConfig.)
          root-level (LogLevel.)
          orig-levels (atom {})
          present (time/now)
          in-thirty-seconds (coerce/to-long (time/plus present (time/secs 30)))
          mock-config-atom (atom {})]
      ;; configure the root logger to be debug
      (doseq [named {"ROOT" "DEBUG"
                     "my_debug_logger" "DEBUG"
                     "my_info_logger" "INFO"
                     "my_error_logger" "ERROR"}]
        (let [level (LogLevel.)]
          (.set_action level LogLevelAction/UPDATE)
          (.set_reset_log_level_timeout_epoch level in-thirty-seconds)
          (.set_target_log_level level (val named))
          (.put_to_named_logger_level mock-config (key named) level)))
      (log-message "Tests " mock-config)
      (stubbing [worker/set-logger-level nil
                 time/now present]
          (worker/process-log-config-change mock-config-atom orig-levels mock-config)
          (verify-call-times-for worker/set-logger-level 4)
          (verify-nth-call-args-for-indices 0 worker/set-logger-level [1 2] "" Level/DEBUG)
          (verify-nth-call-args-for-indices 1 worker/set-logger-level [1 2] "my_debug_logger" Level/DEBUG)
          (verify-nth-call-args-for-indices 2 worker/set-logger-level [1 2] "my_error_logger" Level/ERROR)
          (verify-nth-call-args-for-indices 3 worker/set-logger-level [1 2] "my_info_logger" Level/INFO)))))

(deftest test-worker-is-connection-ready
  (let [connection (Mockito/mock ConnectionWithStatus)]
    (. (Mockito/when (.status connection)) thenReturn ConnectionWithStatus$Status/Ready)
    (is (= true (worker/is-connection-ready connection)))

    (. (Mockito/when (.status connection)) thenReturn ConnectionWithStatus$Status/Connecting)
    (is (= false (worker/is-connection-ready connection)))

    (. (Mockito/when (.status connection)) thenReturn ConnectionWithStatus$Status/Closed)
    (is (= false (worker/is-connection-ready connection)))
  ))
