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
  (:import [backtype.storm.messaging TransportFactory TaskMessage])
  (:import [java.util.concurrent ExecutionException])
  (:use [backtype.storm bootstrap testing util]))

(bootstrap)

(def port 6700)
(def task 1)

(deftest test-basic
  (let [req_msg (String. "0123456789abcdefghijklmnopqrstuvwxyz")
        storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
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
        _ (.send client task (.getBytes req_msg))
        iter (.recv server 0 0)
        resp (.next iter)]
    (is (= task (.task resp)))
    (is (= req_msg (String. (.message resp))))
    (.close client)
    (.close server)
    (.term context)))

(deftest test-large-msg
  (let [req_msg (apply str (repeat 2048000 'c'))
        storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
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
        _ (.send client task (.getBytes req_msg))
        iter (.recv server 0 0)
        resp (.next iter)]
    (is (= task (.task resp)))
    (is (= req_msg (String. (.message resp))))
    (.close client)
    (.close server)
    (.term context)))

(deftest test-server-delayed
    (let [req_msg (String. "0123456789abcdefghijklmnopqrstuvwxyz")
       storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                    }
        context (TransportFactory/makeContext storm-conf)
        client (.connect context nil "localhost" port)

        server (Thread.
                (fn []
                  (Thread/sleep 1000)
                  (let [server (.bind context nil port)
                        iter (.recv server 0 0)
                        resp (.next iter)]
                    (is (= task (.task resp)))
                    (is (= req_msg (String. (.message resp))))
                    (.close server)
                  )))
        _ (.start server)
        _ (.send client task (.getBytes req_msg))
        ]
    (.close client)
    (.join server)
    (.term context)))

(deftest test-batch
  (let [storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024000
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                    }
        context (TransportFactory/makeContext storm-conf)
        server (.bind context nil port)
        client (.connect context nil "localhost" port)]
    (doseq [num  (range 1 100000)]
      (let [req_msg (str num)]
        (.send client task (.getBytes req_msg))))

    (let [resp (ArrayList.)
          received (atom 0)]
      (while (< @received (- 100000 1))
        (let [iter (.recv server 0 0)]
          (while (.hasNext iter)
            (let [msg (.next iter)]
              (.add resp msg)
              (swap! received inc)
              ))))
      (doseq [num  (range 1 100000)]
      (let [req_msg (str num)
            resp_msg (String. (.message (.get resp (- num 1))))]
        (is (= req_msg resp_msg)))))

    (.close client)
    (.close server)
    (.term context)))


(deftest test-reconnect-to-permanently-failed-server
  "Tests that if connection to a server already established and server fails, then
  Client#connect() throws an exception"
  (let [dummy_msg (String. "0123456789abcdefghijklmnopqrstuvwxyz")
        poison_msg (String. "kill_the_server")
        storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024
                    STORM-MESSAGING-NETTY-MAX-RETRIES 5 ; just to decrease test duration
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                    ;critical for this test
                    ;                    STORM-NETTY-MESSAGE-BATCH-SIZE 1
                    }
        context (TransportFactory/makeContext storm-conf)
        client (.connect context nil "localhost" port)
        server_fn (fn []
                    (let [server (.bind context nil port)
                          poll (fn []
                                 (let [iter (.recv server 0 0)
                                       result ()]
                                   (if (nil? iter) () (iterator-seq iter))
                                   ))
                          process_msg (fn [msg]
                                        (let [msg_body (String. (.message msg))]
                                          (if (= poison_msg msg_body)
                                            (do (print "Received a poison...")
                                              true)
                                            (do (is (= dummy_msg msg_body))
                                              (println (str "Received: " msg_body))
                                              (Thread/sleep 100)
                                              false))
                                          ))
                          ]
                      (loop [need_exit false]
                        (when (or (false? need_exit) (nil? need_exit))
                          (recur (some true? (map process_msg (poll))))))
                      ;                          (recur (some true?  (poll)))))
                      (.close server)
                      (println "SERVER CLOSED")
                      ))
        stop_server (fn [server_future]
                      (.send client task (.getBytes poison_msg))
                      (if (= "timeout" (deref server_future 5000 "timeout"))
                        (do
                          ;Note, that this does not stop Server thread
                          ;because of ignoring InterruptedException in Server#recv (what is strange)
                          (future-cancel server_future)
                          (throw (RuntimeException. "Error. Server didn't stop as we asked it."))
                          ))
                      )
        server_1 (future (server_fn))
        _ (println "Let the client connect to a server initially")
        _ (.send client task (.getBytes dummy_msg))
        _ (println "Permanently stopping the server")
        _ (stop_server server_1)
        _ (println "Sending batch of messages to the dead server")
        batch (future (.send client (.iterator [(TaskMessage. task (.getBytes dummy_msg))
                                                (TaskMessage. task (.getBytes dummy_msg))
                                                (TaskMessage. task (.getBytes dummy_msg))
                                                (TaskMessage. task (.getBytes dummy_msg))])))
        _ (is
            (thrown-cause-with-msg? ExecutionException #".*Remote address is not reachable\. We will close this client.*"
              (deref batch (* 2 (* (get storm-conf STORM-MESSAGING-NETTY-MAX-RETRIES) (get storm-conf STORM-MESSAGING-NETTY-MIN-SLEEP-MS))) "timeout")))
        _ (future-cancel batch)
        ]
    (.close client)
    (.term context)))

(deftest test-reconnect-to-temporarily-failed-server
  "Tests that if connection to a server already established and server TEMPORARILY fails, then
  Client#connect() succeeds after several re-tries"
  (let [dummy_msg (String. "0123456789abcdefghijklmnopqrstuvwxyz")
        poison_msg (String. "kill_the_server")
        storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024
                    STORM-MESSAGING-NETTY-MAX-RETRIES 50 ; important for this test
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                    ;critical for this test
                    ;                    STORM-NETTY-MESSAGE-BATCH-SIZE 1
                    }
        context (TransportFactory/makeContext storm-conf)
        client (.connect context nil "localhost" port)

        server_fn (fn []
                    (let [server (.bind context nil port)
                          poll (fn []
                                 (let [iter (.recv server 0 0)
                                       result ()]
                                   (if (nil? iter) () (iterator-seq iter))
                                   ))
                          process_msg (fn [msg]
                                        (let [msg_body (String. (.message msg))]
                                          (if (= poison_msg msg_body)
                                            (do (print "Received a poison...")
                                              true)
                                            (do (is (= dummy_msg msg_body))
                                              (println (str "Received: " msg_body))
                                              (Thread/sleep 100)
                                              false))
                                          ))
                          ]
                      (loop [need_exit false]
                        (when (or (false? need_exit) (nil? need_exit))
                          (recur (some true? (map process_msg (poll))))))
                      ;                          (recur (some true?  (poll)))))
                      (.close server)
                      (println "SERVER CLOSED")
                      ))
        stop_server (fn [server_future]
                      (.send client task (.getBytes poison_msg))

                      (if (= "timeout" (deref server_future 5000 "timeout"))
                        (do
                          ;Note, that this does not stop Server thread
                          ;because of ignoring InterruptedException in Server#recv (what is strange)
                          (future-cancel server_future)
                          (throw (RuntimeException. "Error. Server didn't stop as we asked it."))
                          ))
                      )
        server_1 (future (server_fn))
        _ (println "Let the client connect to a server initially")
        _ (.send client task (.getBytes dummy_msg))
        _ (println "Closing the server")
        _ (stop_server server_1)
        _ (println "Connecting to the temporarily dead server")
        _ (let [reconnect (future (.send client task (.getBytes dummy_msg)))
                _ (print "Sleeping for 10 seconds before resuming the server...")
                _ (Thread/sleep 10000)
                server_2 (future (server_fn))
                _ (println "RESUMED. Expecting that client will send the message successfully.")
                _ (if (= "timeout" (deref reconnect 15000 "timeout"))
                    (do
                      (future-cancel reconnect)
                      "timeout")
                    )
                ]
            (stop_server server_2)
            )
        ]
    (.close client)
    (.term context)))
