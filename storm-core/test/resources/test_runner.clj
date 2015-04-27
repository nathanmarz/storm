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

(ns org.apache.storm.testrunner)

(import `java.util.Properties)
(import `java.io.ByteArrayOutputStream)
(import `java.io.FileInputStream)
(import `java.io.FileOutputStream)
(import `java.io.FileWriter)
(import `java.io.File)
(import `java.io.OutputStream)
(import `java.io.PrintStream)
(import `java.io.PrintWriter)
(use 'clojure.test)
(use 'clojure.test.junit)

(def props (Properties.))
(.load props (FileInputStream. (first *command-line-args*)))

(def namespaces  (into [] 
                       (for [[key val] props
                             :when (.startsWith key "ns.")]
                               (symbol val))))

(def output-dir (.get props "outputDir"))

(dorun (for [ns namespaces]
  (require ns)))

(.mkdirs (File. output-dir))

(let [sys-out System/out
      sys-err System/err
      num-bad (atom 0)
      original-junit-report junit-report
      cached-out (atom nil)
      test-error (atom 0)
      orig-out *out*]
  (dorun (for [ns namespaces]
    (with-open [writer (FileWriter. (str output-dir "/" ns ".xml"))
                out-stream (FileOutputStream. (str output-dir "/" ns "-output.txt"))
                out-writer (PrintStream. out-stream true)]
      (.println sys-out (str "Running " ns))
      (try
        (System/setOut out-writer)
        (System/setErr out-writer)
        (binding [*test-out* writer
                  *out* (PrintWriter. out-stream true)
                  junit-report (fn [data]
                                   (let [type (data :type)]
                                     (cond
                                       (= type :begin-test-var) (do
                                                                   (reset! cached-out (ByteArrayOutputStream.))
                                                                   (let [writer (PrintStream. @cached-out true)]
                                                                     (set! *out* (PrintWriter. @cached-out true))
                                                                     (System/setOut writer)
                                                                     (System/setErr writer))
                                                                   (reset! test-error 0))
                                       (= type :end-test-var) (do
                                                                  (.write out-writer (.toByteArray @cached-out))
                                                                  (when (> @test-error 0)
                                                                    (with-test-out
                                                                      (start-element 'system-out true)
                                                                      (element-content (String. (.toByteArray @cached-out)))
                                                                      (finish-element 'system-out true))))
                                       (= type :fail) (swap! test-error inc)
                                       (= type :error) (swap! test-error inc)))
                                   (original-junit-report data))]
          (with-junit-output
            (let [result (run-tests ns)]
               (.println sys-out (str "Tests run: " (result :test) ", Passed: " (result :pass) ", Failures: " (result :fail) ", Errors: " (result :error)))
               (reset! num-bad (+ @num-bad (result :error) (result :fail))))))
        (finally 
          (System/setOut sys-out)
          (System/setErr sys-err))))))
  (shutdown-agents)
  (System/exit (if (> @num-bad 0) 1 0)))
