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
  (:use [backtype.storm.ui helpers])
  (:import [java.nio.file Files])
  (:import [java.nio.file.attribute FileAttribute])
  (:import [java.io File])
  (:import [org.mockito Mockito]))

(defmulti mk-mock-File #(:type %))

(defmethod mk-mock-File :file [{file-name :name mtime :mtime length :length
                                :or {file-name "afile"
                                     mtime 1
                                     length (* 10 (* 1024 (* 1024 1024))) }}] ; Length 10 GB
  (let [mockFile (Mockito/mock java.io.File)]
    (. (Mockito/when (.getName mockFile)) thenReturn file-name)
    (. (Mockito/when (.lastModified mockFile)) thenReturn mtime)
    (. (Mockito/when (.isFile mockFile)) thenReturn true)
    (. (Mockito/when (.getCanonicalPath mockFile))
      thenReturn (str "/mock/canonical/path/to/" file-name))
    (. (Mockito/when (.length mockFile)) thenReturn length)
    mockFile))

(defmethod mk-mock-File :directory [{dir-name :name mtime :mtime files :files
                                     :or {dir-name "adir" mtime 1 files []}}]
  (let [mockDir (Mockito/mock java.io.File)]
    (. (Mockito/when (.getName mockDir)) thenReturn dir-name)
    (. (Mockito/when (.lastModified mockDir)) thenReturn mtime)
    (. (Mockito/when (.isFile mockDir)) thenReturn false)
    (. (Mockito/when (.listFiles mockDir)) thenReturn (into-array File files))
    mockDir))

(deftest test-mk-FileFilter-for-log-cleanup
  (testing "log file filter selects the correct worker-log dirs for purge"
    (let [now-millis (current-time-millis)
          conf {LOGVIEWER-CLEANUP-AGE-MINS 60
                LOGVIEWER-CLEANUP-INTERVAL-SECS 300}
          cutoff-millis (logviewer/cleanup-cutoff-age-millis conf now-millis)
          old-mtime-millis (- cutoff-millis 500)
          new-mtime-millis (+ cutoff-millis 500)
          matching-files (map #(mk-mock-File %)
                           [{:name "3031"
                             :type :directory
                             :mtime old-mtime-millis}
                            {:name "3032"
                             :type :directory
                             :mtime old-mtime-millis}
                            {:name "7077"
                             :type :directory
                             :mtime old-mtime-millis}])
          excluded-files (map #(mk-mock-File %)
                           [{:name "oldlog-1-2-worker-.log"
                             :type :file
                             :mtime old-mtime-millis}
                            {:name "newlog-1-2-worker.log"
                             :type :file
                             :mtime new-mtime-millis}
                            {:name "some-old-file.txt"
                             :type :file
                             :mtime old-mtime-millis}
                            {:name "olddir-1-2-worker.log"
                             :type :directory
                             :mtime new-mtime-millis}
                            {:name "metadata"
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

(deftest test-sort-worker-logs
  (testing "cleaner sorts the log files in ascending ages for deletion"
    (stubbing [logviewer/filter-candidate-files (fn [x _] x)]
      (let [now-millis (current-time-millis)
            files1 (into-array File (map #(mk-mock-File {:name (str %)
                                                         :type :file
                                                         :mtime (- now-millis (* 100 %))})
                                      (range 1 6)))
            files2 (into-array File (map #(mk-mock-File {:name (str %)
                                                         :type :file
                                                         :mtime (- now-millis (* 100 %))})
                                      (range 6 11)))
            files3 (into-array File (map #(mk-mock-File {:name (str %)
                                                         :type :file
                                                         :mtime (- now-millis (* 100 %))})
                                      (range 11 16)))
            port1-dir (mk-mock-File {:name "/workers-artifacts/topo1/port1"
                                     :type :directory
                                     :files files1})
            port2-dir (mk-mock-File {:name "/workers-artifacts/topo1/port2"
                                     :type :directory
                                     :files files2})
            port3-dir (mk-mock-File {:name "/workers-artifacts/topo2/port3"
                                     :type :directory
                                     :files files3})
            topo1-files (into-array File [port1-dir port2-dir])
            topo2-files (into-array File [port3-dir])
            topo1-dir (mk-mock-File {:name "/workers-artifacts/topo1"
                                     :type :directory
                                     :files topo1-files})
            topo2-dir (mk-mock-File {:name "/workers-artifacts/topo2"
                                     :type :directory
                                     :files topo2-files})
            root-files (into-array File [topo1-dir topo2-dir])
            root-dir (mk-mock-File {:name "/workers-artifacts"
                                    :type :directory
                                    :files root-files})
            sorted-logs (logviewer/sorted-worker-logs root-dir)
            sorted-ints (map #(Integer. (.getName %)) sorted-logs)]
        (is (= (count sorted-logs) 15))
        (is (= (count sorted-ints) 15))
        (is (apply #'> sorted-ints))))))

(deftest test-per-workerdir-cleanup
  (testing "cleaner deletes oldest files in each worker dir if files are larger than per-dir quota."
    (stubbing [rmr nil]
      (let [now-millis (current-time-millis)
            files1 (into-array File (map #(mk-mock-File {:name (str "A" %)
                                                         :type :file
                                                         :mtime (+ now-millis (* 100 %))
                                                         :length 200 })
                                      (range 0 10)))
            files2 (into-array File (map #(mk-mock-File {:name (str "B" %)
                                                         :type :file
                                                         :mtime (+ now-millis (* 100 %))
                                                         :length 200 })
                                      (range 0 10)))
            files3 (into-array File (map #(mk-mock-File {:name (str "C" %)
                                                         :type :file
                                                         :mtime (+ now-millis (* 100 %))
                                                         :length 200 })
                                      (range 0 10)))
            port1-dir (mk-mock-File {:name "/workers-artifacts/topo1/port1"
                                     :type :directory
                                     :files files1})
            port2-dir (mk-mock-File {:name "/workers-artifacts/topo1/port2"
                                     :type :directory
                                     :files files2})
            port3-dir (mk-mock-File {:name "/workers-artifacts/topo2/port3"
                                     :type :directory
                                     :files files3})
            topo1-files (into-array File [port1-dir port2-dir])
            topo2-files (into-array File [port3-dir])
            topo1-dir (mk-mock-File {:name "/workers-artifacts/topo1"
                                     :type :directory
                                     :files topo1-files})
            topo2-dir (mk-mock-File {:name "/workers-artifacts/topo2"
                                     :type :directory
                                     :files topo2-files})
            root-files (into-array File [topo1-dir topo2-dir])
            root-dir (mk-mock-File {:name "/workers-artifacts"
                                    :type :directory
                                    :files root-files})
            remaining-logs (logviewer/per-workerdir-cleanup root-dir 1200)]
        (is (= (count (first remaining-logs)) 6))
        (is (= (count (second remaining-logs)) 6))
        (is (= (count (last remaining-logs)) 6))))))

(deftest test-delete-oldest-log-cleanup
  (testing "delete oldest logs deletes the oldest set of logs when the total size gets too large."
    (stubbing [rmr nil]
      (let [now-millis (current-time-millis)
            files (into-array File (map #(mk-mock-File {:name (str %)
                                                        :type :file
                                                        :mtime (+ now-millis (* 100 %))
                                                        :length 100 })
                                     (range 0 20)))
            remaining-logs (logviewer/delete-oldest-while-logs-too-large files 501)]
        (is (= (logviewer/sum-file-size files) 2000))
        (is (= (count remaining-logs) 5))
        (is (= (.getName (first remaining-logs)) "15"))))))

(deftest test-identify-worker-log-dirs
  (testing "Build up workerid-workerlogdir map for the old workers' dirs"
    (let [port1-dir (mk-mock-File {:name "/workers-artifacts/topo1/port1"
                                   :type :directory})
          mock-metaFile (mk-mock-File {:name "worker.yaml"
                                       :type :file})
          exp-id "id12345"
          expected {exp-id port1-dir}]
      (stubbing [supervisor/read-worker-heartbeats nil
                 logviewer/get-metadata-file-for-wroker-logdir mock-metaFile
                 logviewer/get-worker-id-from-metadata-file exp-id]
        (is (= expected (logviewer/identify-worker-log-dirs [port1-dir])))))))

(deftest test-get-dead-worker-dirs
  (testing "removes any files of workers that are still alive"
    (let [conf {SUPERVISOR-WORKER-TIMEOUT-SECS 5}
          id->hb {"42" {:time-secs 1}}
          now-secs 2
          unexpected-dir (mk-mock-File {:name "dir1" :type :directory})
          expected-dir (mk-mock-File {:name "dir2" :type :directory})
          log-dirs #{unexpected-dir expected-dir}]
      (stubbing [logviewer/identify-worker-log-dirs {"42" unexpected-dir,
                                                     "007" expected-dir}
                 supervisor/read-worker-heartbeats id->hb]
        (is (= #{expected-dir}
              (logviewer/get-dead-worker-dirs conf now-secs log-dirs)))))))

(deftest test-cleanup-fn
  (testing "cleanup function rmr's files of dead workers"
    (let [mockfile1 (mk-mock-File {:name "delete-me1" :type :file})
          mockfile2 (mk-mock-File {:name "delete-me2" :type :file})]
      (stubbing [logviewer/select-dirs-for-cleanup nil
                 logviewer/get-dead-worker-dirs (sorted-set mockfile1 mockfile2)
                 logviewer/cleanup-empty-topodir nil
                 rmr nil]
        (logviewer/cleanup-fn! "/bogus/path")
        (verify-call-times-for rmr 2)
        (verify-nth-call-args-for 1 rmr (.getCanonicalPath mockfile1))
        (verify-nth-call-args-for 2 rmr (.getCanonicalPath mockfile2))))))

(deftest test-authorized-log-user
  (testing "allow cluster admin"
    (let [conf {NIMBUS-ADMINS ["alice"]}]
      (stubbing [logviewer/get-log-user-group-whitelist [[] []]
                 logviewer/user-groups []]
        (is (logviewer/authorized-log-user? "alice" "non-blank-fname" conf))
        (verify-first-call-args-for logviewer/get-log-user-group-whitelist "non-blank-fname")
        (verify-first-call-args-for logviewer/user-groups "alice"))))

  (testing "ignore any cluster-set topology.users topology.groups"
    (let [conf {TOPOLOGY-USERS ["alice"]
                TOPOLOGY-GROUPS ["alice-group"]}]
      (stubbing [logviewer/get-log-user-group-whitelist [[] []]
                 logviewer/user-groups ["alice-group"]]
        (is (not (logviewer/authorized-log-user? "alice" "non-blank-fname" conf)))
        (verify-first-call-args-for logviewer/get-log-user-group-whitelist "non-blank-fname")
        (verify-first-call-args-for logviewer/user-groups "alice"))))

  (testing "allow cluster logs user"
    (let [conf {LOGS-USERS ["alice"]}]
      (stubbing [logviewer/get-log-user-group-whitelist [[] []]
                 logviewer/user-groups []]
        (is (logviewer/authorized-log-user? "alice" "non-blank-fname" conf))
        (verify-first-call-args-for logviewer/get-log-user-group-whitelist "non-blank-fname")
        (verify-first-call-args-for logviewer/user-groups "alice"))))

  (testing "allow whitelisted topology user"
    (stubbing [logviewer/get-log-user-group-whitelist [["alice"] []]
               logviewer/user-groups []]
      (is (logviewer/authorized-log-user? "alice" "non-blank-fname" {}))
      (verify-first-call-args-for logviewer/get-log-user-group-whitelist "non-blank-fname")
      (verify-first-call-args-for logviewer/user-groups "alice")))

  (testing "allow whitelisted topology group"
    (stubbing [logviewer/get-log-user-group-whitelist [[] ["alice-group"]]
               logviewer/user-groups ["alice-group"]]
      (is (logviewer/authorized-log-user? "alice" "non-blank-fname" {}))
      (verify-first-call-args-for logviewer/get-log-user-group-whitelist "non-blank-fname")
      (verify-first-call-args-for logviewer/user-groups "alice")))

  (testing "disallow user not in nimbus admin, topo user, logs user, or whitelist"
    (stubbing [logviewer/get-log-user-group-whitelist [[] []]
               logviewer/user-groups []]
      (is (not (logviewer/authorized-log-user? "alice" "non-blank-fname" {})))
      (verify-first-call-args-for logviewer/get-log-user-group-whitelist "non-blank-fname")
      (verify-first-call-args-for logviewer/user-groups "alice"))))

(deftest test-list-log-files
  (testing "list-log-files filter selects the correct log files to return"
    (let [attrs (make-array FileAttribute 0)
          root-path (.getCanonicalPath (.toFile (Files/createTempDirectory "workers-artifacts" attrs)))
          file1 (clojure.java.io/file root-path "topoA" "port1" "worker.log")
          file2 (clojure.java.io/file root-path "topoA" "port2" "worker.log")
          file3 (clojure.java.io/file root-path "topoB" "port1" "worker.log")
          _ (clojure.java.io/make-parents file1)
          _ (clojure.java.io/make-parents file2)
          _ (clojure.java.io/make-parents file3)
          _ (.createNewFile file1)
          _ (.createNewFile file2)
          _ (.createNewFile file3)
          origin "www.origin.server.net"
          expected-all (json-response '("topoA/port1/worker.log" "topoA/port2/worker.log"
                                         "topoB/port1/worker.log")
                         nil
                         :headers {"Access-Control-Allow-Origin" origin
                                   "Access-Control-Allow-Credentials" "true"})
          expected-filter-port (json-response '("topoA/port1/worker.log" "topoB/port1/worker.log")
                                 nil
                                 :headers {"Access-Control-Allow-Origin" origin
                                           "Access-Control-Allow-Credentials" "true"})
          expected-filter-topoId (json-response '("topoB/port1/worker.log")
                                   nil
                                   :headers {"Access-Control-Allow-Origin" origin
                                             "Access-Control-Allow-Credentials" "true"})
          returned-all (logviewer/list-log-files "user" nil nil root-path nil origin)
          returned-filter-port (logviewer/list-log-files "user" nil "port1" root-path nil origin)
          returned-filter-topoId (logviewer/list-log-files "user" "topoB" nil root-path nil origin)]
      (rmr root-path)
      (is   (= expected-all returned-all))
      (is   (= expected-filter-port returned-filter-port))
      (is   (= expected-filter-topoId returned-filter-topoId)))))
