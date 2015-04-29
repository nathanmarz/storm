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
(ns backtype.storm.messaging.netty-unit-test
  (:use [clojure test])
  (:import [backtype.storm.messaging TransportFactory])
  (:use [backtype.storm testing util config log])
  (:use [backtype.storm.daemon.worker :only [is-connection-ready]])
  (:import [java.util ArrayList]))

(def port 6700)
(def task 1)

;; In a "real" cluster (or an integration test), Storm itself would ensure that a topology's workers would only be
;; activated once all the workers' connections are ready.  The tests in this file however launch Netty servers and
;; clients directly, and thus we must ensure manually that the server and the client connections are ready before we
;; commence testing.  If we don't do this, then we will lose the first messages being sent between the client and the
;; server, which will fail the tests.
(defn- wait-until-ready
  ([connections]
      (do (log-message "Waiting until all Netty connections are ready...")
          (wait-until-ready connections 0)))
  ([connections waited-ms]
    (let [interval-ms 10
          max-wait-ms 5000]
      (if-not (every? is-connection-ready connections)
        (if (<= waited-ms max-wait-ms)
          (do
            (Thread/sleep interval-ms)
            (wait-until-ready connections (+ waited-ms interval-ms)))
          (throw (RuntimeException. (str "Netty connections were not ready within " max-wait-ms " ms"))))
        (log-message "All Netty connections are ready")))))

(deftest test-basic
  (log-message "Should send and receive a basic message")
  (let [req_msg (String. "0123456789abcdefghijklmnopqrstuvwxyz")
        storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-AUTHENTICATION false
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                    }
        context (TransportFactory/makeContext storm-conf)
        server (.bind context nil port)
        client (.connect context nil "localhost" port)
        _ (wait-until-ready [server client])
        _ (.send client task (.getBytes req_msg))
        iter (.recv server 0 0)
        resp (.next iter)]
    (is (= task (.task resp)))
    (is (= req_msg (String. (.message resp))))
    (.close client)
    (.close server)
    (.term context)))

(deftest test-large-msg
  (log-message "Should send and receive a large message")
  (let [req_msg (apply str (repeat 2048000 'c'))
        storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-AUTHENTICATION false
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 102400
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                    }
        context (TransportFactory/makeContext storm-conf)
        server (.bind context nil port)
        client (.connect context nil "localhost" port)
        _ (wait-until-ready [server client])
        _ (.send client task (.getBytes req_msg))
        iter (.recv server 0 0)
        resp (.next iter)]
    (is (= task (.task resp)))
    (is (= req_msg (String. (.message resp))))
    (.close client)
    (.close server)
    (.term context)))


(deftest test-batch
  (let [num-messages 100000
        storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-AUTHENTICATION false
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024000
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                    }
        _ (log-message "Should send and receive many messages (testing with " num-messages " messages)")
        context (TransportFactory/makeContext storm-conf)
        server (.bind context nil port)
        client (.connect context nil "localhost" port)
        _ (wait-until-ready [server client])]
    (doseq [num  (range 1 num-messages)]
      (let [req_msg (str num)]
        (.send client task (.getBytes req_msg))))

    (let [resp (ArrayList.)
          received (atom 0)]
      (while (< @received (- num-messages 1))
        (let [iter (.recv server 0 0)]
          (while (.hasNext iter)
            (let [msg (.next iter)]
              (.add resp msg)
              (swap! received inc)
              ))))
      (doseq [num  (range 1 num-messages)]
      (let [req_msg (str num)
            resp_msg (String. (.message (.get resp (- num 1))))]
        (is (= req_msg resp_msg)))))

    (.close client)
    (.close server)
    (.term context)))
