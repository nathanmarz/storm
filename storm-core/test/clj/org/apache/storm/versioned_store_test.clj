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
(ns org.apache.storm.versioned-store-test
  (:use [clojure test])
  (:use [org.apache.storm testing])
  (:import [java.io File])
  (:import [org.apache.commons.io FileUtils])
  (:import [org.apache.storm.utils VersionedStore]))

(defmacro defvstest [name [vs-sym] & body]
  `(deftest ~name
    (with-local-tmp [dir#]
      (let [~vs-sym (VersionedStore. dir#)]
        ~@body
        ))))

(defn writeToFile [^String file]
  (FileUtils/writeStringToFile (File. file) (str "time:" (System/currentTimeMillis))))

(defvstest test-empty-version [vs]
  (let [v (.createVersion vs)]
    (.succeedVersion vs v)
    (writeToFile v)
    (is (= 1 (count (.getAllVersions vs))))
    (is (= v (.mostRecentVersionPath vs)))
    ))

(defvstest test-multiple-versions [vs]
  (let [v (.createVersion vs)]
    (.succeedVersion vs v)
    (writeToFile v))
  (Thread/sleep 100)
  (let [v (.createVersion vs)]
    (.succeedVersion vs v)
    (writeToFile v)
    (is (= 2 (count (.getAllVersions vs))))
    (is (= v (.mostRecentVersionPath vs)))
    
    (.createVersion vs)
    (is (= v (.mostRecentVersionPath vs)))
    ))
