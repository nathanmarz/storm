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
