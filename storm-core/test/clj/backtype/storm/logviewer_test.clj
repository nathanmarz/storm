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
(ns backtype.storm.logviewer-test
  (:use [backtype.storm config util])
  (:require [backtype.storm.daemon [logviewer :as logviewer]
                                   [supervisor :as supervisor]])
  (:require [conjure.core])
  (:use [clojure test])
  (:use [conjure core])
  (:import [org.mockito Mockito]))

(defmulti mk-mock-File #(:type %))

(defmethod mk-mock-File :file [{file-name :name mtime :mtime
                                :or {file-name "afile" mtime 1}}]
  (let [mockFile (Mockito/mock java.io.File)]
    (. (Mockito/when (.getName mockFile)) thenReturn file-name)
    (. (Mockito/when (.lastModified mockFile)) thenReturn mtime)
    (. (Mockito/when (.isFile mockFile)) thenReturn true)
    (. (Mockito/when (.getCanonicalPath mockFile))
       thenReturn (str "/mock/canonical/path/to/" file-name))
    mockFile))

(defmethod mk-mock-File :directory [{dir-name :name mtime :mtime
                                     :or {dir-name "adir" mtime 1}}]
  (let [mockDir (Mockito/mock java.io.File)]
    (. (Mockito/when (.getName mockDir)) thenReturn dir-name)
    (. (Mockito/when (.lastModified mockDir)) thenReturn mtime)
    (. (Mockito/when (.isFile mockDir)) thenReturn false)
    mockDir))

(deftest test-mk-FileFilter-for-log-cleanup
  (testing "log file filter selects the correct log files for purge"
    (let [now-millis (current-time-millis)
          conf {LOGVIEWER-CLEANUP-AGE-MINS 60
                LOGVIEWER-CLEANUP-INTERVAL-SECS 300}
          cutoff-millis (logviewer/cleanup-cutoff-age-millis conf now-millis)
          old-mtime-millis (- cutoff-millis 500)
          new-mtime-millis (+ cutoff-millis 500)
          matching-files (map #(mk-mock-File %)
                              [{:name "oldlog-1-2-worker-3.log"
                                :type :file
                                :mtime old-mtime-millis}
                               {:name "oldlog-1-2-worker-3.log.8"
                                :type :file
                                :mtime old-mtime-millis}
                               {:name "foobar*_topo-1-24242-worker-2834238.log"
                                :type :file
                                :mtime old-mtime-millis}])
          excluded-files (map #(mk-mock-File %)
                              [{:name "oldlog-1-2-worker-.log"
                                :type :file
                                :mtime old-mtime-millis}
                               {:name "olddir-1-2-worker.log"
                                :type :directory
                                :mtime old-mtime-millis}
                               {:name "newlog-1-2-worker.log"
                                :type :file
                                :mtime new-mtime-millis}
                               {:name "some-old-file.txt"
                                :type :file
                                :mtime old-mtime-millis}
                               {:name "metadata"
                                :type :directory
                                :mtime old-mtime-millis}
                               {:name "newdir-1-2-worker.log"
                                :type :directory
                                :mtime new-mtime-millis}
                               {:name "newdir"
                                :type :directory
                                :mtime new-mtime-millis}
                              ])
          file-filter (logviewer/mk-FileFilter-for-log-cleanup conf now-millis)]
        (is   (every? #(.accept file-filter %) matching-files))
        (is (not-any? #(.accept file-filter %) excluded-files))
      )))

(deftest test-get-log-root->files-map
  (testing "returns map of root name to list of files"
    (let [files (vec (map #(java.io.File. %) ["log-1-2-worker-3.log"
                                              "log-1-2-worker-3.log.1"
                                              "log-2-4-worker-6.log.1"]))
          expected {"log-1-2-worker-3" #{(files 0) (files 1)}
                    "log-2-4-worker-6" #{(files 2)}}]
      (is (= expected (logviewer/get-log-root->files-map files))))))

(deftest test-identify-worker-log-files
  (testing "Does not include metadata file when there are any log files that
           should not be cleaned up"
    (let [cutoff-millis 2000
          old-logFile (mk-mock-File {:name "mock-1-1-worker-1.log.1"
                                     :type :file
                                     :mtime (- cutoff-millis 1000)})
          mock-metaFile (mk-mock-File {:name "mock-1-1-worker-1.yaml"
                                       :type :file
                                       :mtime 1})
          new-logFile (mk-mock-File {:name "mock-1-1-worker-1.log"
                                     :type :file
                                     :mtime (+ cutoff-millis 1000)})
          exp-id "id12345"
          exp-user "alice"
          expected {exp-id {:owner exp-user
                            :files #{old-logFile}}}]
      (stubbing [supervisor/read-worker-heartbeats nil
                logviewer/get-metadata-file-for-log-root-name mock-metaFile
                read-dir-contents [(.getName old-logFile) (.getName new-logFile)]
                logviewer/get-worker-id-from-metadata-file exp-id
                logviewer/get-topo-owner-from-metadata-file exp-user]
        (is (= expected (logviewer/identify-worker-log-files [old-logFile] "/tmp/")))))))

(deftest test-get-dead-worker-files-and-owners
  (testing "removes any files of workers that are still alive"
    (let [conf {SUPERVISOR-WORKER-TIMEOUT-SECS 5}
          id->hb {"42" {:time-secs 1}}
          now-secs 2
          log-files #{:expected-file :unexpected-file}
          exp-owner "alice"]
      (stubbing [logviewer/identify-worker-log-files {"42" {:owner exp-owner
                                                            :files #{:unexpected-file}}
                                                      "007" {:owner exp-owner
                                                             :files #{:expected-file}}}
                 logviewer/get-topo-owner-from-metadata-file "alice"
                 supervisor/read-worker-heartbeats id->hb]
        (is (= [{:owner exp-owner :files #{:expected-file}}]
               (logviewer/get-dead-worker-files-and-owners conf now-secs log-files "/tmp/")))))))

(deftest test-cleanup-fn
  (testing "cleanup function removes file as user when one is specified"
    (let [exp-user "mock-user"
          mockfile1 (mk-mock-File {:name "file1" :type :file})
          mockfile2 (mk-mock-File {:name "file2" :type :file})
          mockfile3 (mk-mock-File {:name "file3" :type :file})
          mockyaml  (mk-mock-File {:name "foo.yaml" :type :file})
          exp-cmd (str "rmr /mock/canonical/path/to/" (.getName mockfile3))]
      (stubbing [logviewer/select-files-for-cleanup
                   [(mk-mock-File {:name "throwaway" :type :file})]
                 logviewer/get-dead-worker-files-and-owners
                   [{:owner nil :files #{mockfile1}}
                    {:files #{mockfile2}}
                    {:owner exp-user :files #{mockfile3 mockyaml}}]
                 supervisor/worker-launcher nil
                 rmr nil]
        (logviewer/cleanup-fn! "/tmp/")
        (verify-call-times-for supervisor/worker-launcher 1)
        (verify-first-call-args-for-indices supervisor/worker-launcher
                                            [1 2] exp-user exp-cmd)
        (verify-call-times-for rmr 3)
        (verify-nth-call-args-for 1 rmr (.getCanonicalPath mockfile1))
        (verify-nth-call-args-for 2 rmr (.getCanonicalPath mockfile2))
        (verify-nth-call-args-for 3 rmr (.getCanonicalPath mockyaml))))))

(deftest test-authorized-log-user
  (testing "allow cluster admin"
    (let [conf {NIMBUS-ADMINS ["alice"]}]
      (stubbing [logviewer/get-log-user-whitelist []]
        (is (logviewer/authorized-log-user? "alice" "non-blank-fname" conf)))))

  (testing "ignore any cluster-set topology.users"
    (let [conf {TOPOLOGY-USERS ["alice"]}]
      (stubbing [logviewer/get-log-user-whitelist []]
        (is (not (logviewer/authorized-log-user? "alice" "non-blank-fname" conf))))))

  (testing "allow cluster logs user"
    (let [conf {LOGS-USERS ["alice"]}]
      (stubbing [logviewer/get-log-user-whitelist []]
        (is (logviewer/authorized-log-user? "alice" "non-blank-fname" conf)))))

  (testing "allow whitelisted topology user"
    (stubbing [logviewer/get-log-user-whitelist ["alice"]]
      (is (logviewer/authorized-log-user? "alice" "non-blank-fname" {}))))

  (testing "disallow user not in nimbus admin, topo user, logs user, or whitelist"
    (stubbing [logviewer/get-log-user-whitelist []]
      (is (not (logviewer/authorized-log-user? "alice" "non-blank-fname" {}))))))
