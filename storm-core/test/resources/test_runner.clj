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
(import `java.io.OutputStreamWriter)
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
      orig-out *out*]
  (dorun (for [ns namespaces]
    (with-open [out-stream (FileOutputStream. (str output-dir "/" ns ".xml"))
                print-writer (PrintWriter. out-stream true)
                print-stream (PrintStream. out-stream true)]
      (.println sys-out (str "Running " ns))
      (try
        (let [in-sys-out (atom false)]
        (binding [*test-out* print-writer
                  *out* orig-out
                  junit-report (fn [data]
                                   (let [type (data :type)]
                                     (cond
                                       (= type :begin-test-var) (do
                                                                  (when @in-sys-out
                                                                    (reset! in-sys-out false)
                                                                    (System/setOut sys-out)
                                                                    (System/setErr sys-err)
                                                                    (set! *out* orig-out)
                                                                    (with-test-out
                                                                      (print "]]>")
                                                                      (finish-element 'system-out true)))
                                                                   (original-junit-report data)
                                                                   (reset! in-sys-out true)
                                                                   (with-test-out
                                                                     (start-element 'system-out true)
                                                                     (print "<![CDATA[") (flush))
                                                                   (System/setOut print-stream)
                                                                   (System/setErr print-stream)
                                                                   (set! *out* print-writer))
                                       (= type :end-test-var) (when @in-sys-out
                                                                (reset! in-sys-out false)
                                                                (System/setOut sys-out)
                                                                (System/setErr sys-err)
                                                                (set! *out* orig-out)
                                                                (with-test-out
                                                                  (print "]]>")
                                                                  (finish-element 'system-out true)))
                                       (= type :fail) (when @in-sys-out
                                                                (reset! in-sys-out false)
                                                                (System/setOut sys-out)
                                                                (System/setErr sys-err)
                                                                (set! *out* orig-out)
                                                                (with-test-out
                                                                  (print "]]>")
                                                                  (finish-element 'system-out true)))
                                       (= type :error) (when @in-sys-out
                                                                (reset! in-sys-out false)
                                                                (System/setOut sys-out)
                                                                (System/setErr sys-err)
                                                                (set! *out* orig-out)
                                                                (with-test-out
                                                                  (print "]]>")
                                                                  (finish-element 'system-out true))))
                                     (if (not (= type :begin-test-var)) (original-junit-report data))))]
          (with-junit-output
            (let [result (run-tests ns)]
               (.println sys-out (str "Tests run: " (result :test) ", Passed: " (result :pass) ", Failures: " (result :fail) ", Errors: " (result :error)))
               (reset! num-bad (+ @num-bad (result :error) (result :fail)))))))
        (finally 
          (System/setOut sys-out)
          (System/setErr sys-err))))))
  (shutdown-agents)
  (System/exit (if (> @num-bad 0) 1 0)))
