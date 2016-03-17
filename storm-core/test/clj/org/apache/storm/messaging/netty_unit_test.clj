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
(ns org.apache.storm.messaging.netty-unit-test
  (:use [clojure test])
  (:import [org.apache.storm.messaging TransportFactory IConnection TaskMessage IConnectionCallback])
  (:import [org.apache.storm.utils Utils])
  (:use [org.apache.storm testing util config log])
  (:use [org.apache.storm.daemon.worker :only [is-connection-ready]])
  (:import [java.util ArrayList]))

(def port (Utils/getAvailablePort))
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
   (loop [connections connections waited-ms waited-ms]
     (let [interval-ms 10
           max-wait-ms 5000]
       (if-not (every? is-connection-ready connections)
         (if (<= waited-ms max-wait-ms)
           (do
             (Thread/sleep interval-ms)
             (recur connections (+ waited-ms interval-ms)))
           (throw (RuntimeException. (str "Netty connections were not ready within " max-wait-ms " ms"))))
         (log-message "All Netty connections are ready"))))))

(defn mk-connection-callback
  "make an IConnectionCallback"
  [my-fn]
  (reify IConnectionCallback
    (recv [this batch]
      (doseq [msg batch]
        (my-fn msg)))))

(defn register-callback
  "register the local-transfer-fn with the server"
  [my-fn ^IConnection socket]
  (.registerRecv socket (mk-connection-callback my-fn)))

(defn- wait-for-not-nil
  [atm]
  (while-timeout TEST-TIMEOUT-MS (nil? @atm) (Thread/sleep 10)))

(defn- test-basic-fn [storm-conf]
  (log-message "1. Should send and receive a basic message")
  (let [req_msg (String. "0123456789abcdefghijklmnopqrstuvwxyz")
        context (TransportFactory/makeContext storm-conf)
        port (Utils/getAvailablePort (int 6700))
        resp (atom nil)
        server (.bind context nil port)
        _ (register-callback (fn [message] (reset! resp message)) server)
        client (.connect context nil "localhost" port)
        _ (wait-until-ready [server client])
        _ (.send client task (.getBytes req_msg))]
    (wait-for-not-nil resp)
    (is (= task (.task @resp)))
    (is (= req_msg (String. (.message @resp))))
    (.close client)
    (.close server)
    (.term context)))

(deftest test-basic
 (let [storm-conf {STORM-MESSAGING-TRANSPORT "org.apache.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-AUTHENTICATION false
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                    TOPOLOGY-KRYO-FACTORY "org.apache.storm.serialization.DefaultKryoFactory"
                    TOPOLOGY-TUPLE-SERIALIZER "org.apache.storm.serialization.types.ListDelegateSerializer"
                    TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false
                    TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS false}
        storm-conf-sasl (assoc storm-conf
                                    STORM-MESSAGING-NETTY-AUTHENTICATION true
                                    TOPOLOGY-NAME "topo1-netty-sasl"
                                    STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD (str (Utils/secureRandomLong) ":" (Utils/secureRandomLong)))]
   (test-basic-fn storm-conf)          ;; test with sasl authentication disabled
   (test-basic-fn storm-conf-sasl)))   ;; test with sasl authentication enabled

(defn- test-load-fn [storm-conf]
  (log-message "2 test load")
  (let [req_msg (String. "0123456789abcdefghijklmnopqrstuvwxyz")
        context (TransportFactory/makeContext storm-conf)
        port (Utils/getAvailablePort (int 6700))
        resp (atom nil)
        server (.bind context nil port)
        _ (register-callback (fn [message] (reset! resp message)) server)
        client (.connect context nil "localhost" port)
        _ (wait-until-ready [server client])
        _ (.send client task (.getBytes req_msg))
        _ (.sendLoadMetrics server {(int 1) 0.0 (int 2) 1.0})
        _ (while-timeout 5000 (empty? (.getLoad client [(int 1) (int 2)])) (Thread/sleep 10))
        load (.getLoad client [(int 1) (int 2)])]
    (is (= 0.0 (.getBoltLoad (.get load (int 1)))))
    (is (= 1.0 (.getBoltLoad (.get load (int 2)))))
    (wait-for-not-nil resp)
    (is (= task (.task @resp)))
    (is (= req_msg (String. (.message @resp))))
    (.close client)
    (.close server)
    (.term context)))

(deftest test-load
 (let [storm-conf {STORM-MESSAGING-TRANSPORT "org.apache.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-AUTHENTICATION false
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                    TOPOLOGY-KRYO-FACTORY "org.apache.storm.serialization.DefaultKryoFactory"
                    TOPOLOGY-TUPLE-SERIALIZER "org.apache.storm.serialization.types.ListDelegateSerializer"
                    TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false
                    TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS false}
        storm-conf-sasl (assoc storm-conf
                                    STORM-MESSAGING-NETTY-AUTHENTICATION true
                                    TOPOLOGY-NAME "topo1-netty-sasl"
                                    STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD (str (Utils/secureRandomLong) ":" (Utils/secureRandomLong)))]
   (test-load-fn storm-conf)          ;; test with sasl authentication disabled
   (test-load-fn storm-conf-sasl)))   ;; test with sasl authentication enabled

(defn test-large-msg-fn [storm-conf]
  (log-message "3 Should send and receive a large message")
  (let [req_msg (apply str (repeat 2048000 'c'))
        context (TransportFactory/makeContext storm-conf)
        port (Utils/getAvailablePort (int 6700))
        resp (atom nil)
        server (.bind context nil port)
        _ (register-callback (fn [message] (reset! resp message)) server)
        client (.connect context nil "localhost" port)
        _ (wait-until-ready [server client])
        _ (.send client task (.getBytes req_msg))]
    (wait-for-not-nil resp)
    (is (= task (.task @resp)))
    (is (= req_msg (String. (.message @resp))))
    (.close client)
    (.close server)
    (.term context)))

(deftest test-large-msg
 (let [storm-conf {STORM-MESSAGING-TRANSPORT "org.apache.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-AUTHENTICATION false
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 102400
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                    TOPOLOGY-KRYO-FACTORY "org.apache.storm.serialization.DefaultKryoFactory"
                    TOPOLOGY-TUPLE-SERIALIZER "org.apache.storm.serialization.types.ListDelegateSerializer"
                    TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false
                    TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS false}
        storm-conf-sasl (assoc storm-conf
                                    STORM-MESSAGING-NETTY-AUTHENTICATION true
                                    TOPOLOGY-NAME "topo1-netty-sasl"
                                    STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD (str (Utils/secureRandomLong) ":" (Utils/secureRandomLong)))]
   (test-large-msg-fn storm-conf)          ;; test with sasl authentication disabled
   (test-large-msg-fn storm-conf-sasl)))   ;; test with sasl authentication enabled

(defn- test-server-delayed-fn [storm-conf]
  (log-message "4. test server delayed")
  (let [req_msg (String. "0123456789abcdefghijklmnopqrstuvwxyz")
        context (TransportFactory/makeContext storm-conf)
        resp (atom nil)
        port (Utils/getAvailablePort (int 6700))
        client (.connect context nil "localhost" port)

        server (Thread.
                 (fn []
                   (Thread/sleep 100)
                   (let [server (.bind context nil port)]
                     (register-callback (fn [message] (reset! resp message)) server))))]
    (.start server)
    (wait-until-ready [server client])
    (.send client task (.getBytes req_msg))

    (wait-for-not-nil resp)
    (is (= task (.task @resp)))
    (is (= req_msg (String. (.message @resp))))

    (.join server)
    (.close client)
    (.term context)))

(deftest test-server-delayed
 (let [storm-conf {STORM-MESSAGING-TRANSPORT "org.apache.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-AUTHENTICATION false
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                    TOPOLOGY-KRYO-FACTORY "org.apache.storm.serialization.DefaultKryoFactory"
                    TOPOLOGY-TUPLE-SERIALIZER "org.apache.storm.serialization.types.ListDelegateSerializer"
                    TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false
                    TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS false}
        storm-conf-sasl (assoc storm-conf
                                    STORM-MESSAGING-NETTY-AUTHENTICATION true
                                    TOPOLOGY-NAME "topo1-netty-sasl"
                                    STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD (str (Utils/secureRandomLong) ":" (Utils/secureRandomLong)))]
   (test-server-delayed-fn storm-conf)          ;; test with sasl authentication disabled
   (test-server-delayed-fn storm-conf-sasl)))   ;; test with sasl authentication enabled


(defn- test-batch-fn [storm-conf]
  (log-message "5. test batch")
  (let [num-messages 100000
        _ (log-message "Should send and receive many messages (testing with " num-messages " messages)")
        resp (ArrayList.)
        received (atom 0)
        context (TransportFactory/makeContext storm-conf)
        port (Utils/getAvailablePort (int 6700))
        server (.bind context nil port)
        _ (register-callback (fn [message] (.add resp message) (swap! received inc)) server)
        client (.connect context nil "localhost" port)
        _ (wait-until-ready [server client])]
    (doseq [num (range 1 num-messages)]
      (let [req_msg (str num)]
        (.send client task (.getBytes req_msg))))

    (while-timeout TEST-TIMEOUT-MS (< (.size resp) (- num-messages 1)) (log-message (.size resp) " " num-messages) (Thread/sleep 10))

    (doseq [num  (range 1 num-messages)]
      (let [req_msg (str num)
            resp_msg (String. (.message (.get resp (- num 1))))]
        (is (= req_msg resp_msg))))

    (.close client)
    (.close server)
    (.term context))

(deftest test-batch
 (let [storm-conf {STORM-MESSAGING-TRANSPORT "org.apache.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-AUTHENTICATION false
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024000
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000
                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                    TOPOLOGY-KRYO-FACTORY "org.apache.storm.serialization.DefaultKryoFactory"
                    TOPOLOGY-TUPLE-SERIALIZER "org.apache.storm.serialization.types.ListDelegateSerializer"
                    TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false
                    TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS false}
        storm-conf-sasl (assoc storm-conf
                                    STORM-MESSAGING-NETTY-AUTHENTICATION true
                                    TOPOLOGY-NAME "topo1-netty-sasl"
                                    STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD (str (Utils/secureRandomLong) ":" (Utils/secureRandomLong)))]
   (test-batch-fn storm-conf)          ;; test with sasl authentication disabled
   (test-batch-fn storm-conf-sasl)))   ;; test with sasl authentication enabled
)

(defn- test-server-always-reconnects-fn [storm-conf]
  (log-message "6. test server always reconnects")
    (let [req_msg (String. "0123456789abcdefghijklmnopqrstuvwxyz")
          storm-conf {STORM-MESSAGING-TRANSPORT "org.apache.storm.messaging.netty.Context"
                      STORM-MESSAGING-NETTY-AUTHENTICATION false
                      STORM-MESSAGING-NETTY-BUFFER-SIZE 1024
                      STORM-MESSAGING-NETTY-MAX-RETRIES 2
                      STORM-MESSAGING-NETTY-MIN-SLEEP-MS 10
                      STORM-MESSAGING-NETTY-MAX-SLEEP-MS 50
                      STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                      STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                      TOPOLOGY-KRYO-FACTORY "org.apache.storm.serialization.DefaultKryoFactory"
                      TOPOLOGY-TUPLE-SERIALIZER "org.apache.storm.serialization.types.ListDelegateSerializer"
                      TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false
                      TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS false}
          resp (atom nil)
          context (TransportFactory/makeContext storm-conf)
          port (Utils/getAvailablePort (int 6700))
          client (.connect context nil "localhost" port)
          _ (.send client task (.getBytes req_msg))
          server (.bind context nil port)
          _ (register-callback (fn [message] (reset! resp message)) server)
          _ (wait-until-ready [server client])
          _ (.send client task (.getBytes req_msg))]
      (wait-for-not-nil resp)
      (is (= task (.task @resp)))
      (is (= req_msg (String. (.message @resp))))
      (.close client)
      (.close server)
      (.term context)))

(deftest test-server-always-reconnects
 (let [storm-conf {STORM-MESSAGING-TRANSPORT "org.apache.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-AUTHENTICATION false
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024
                    STORM-MESSAGING-NETTY-MAX-RETRIES 2
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 10
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 50
                    STORM-MESSAGING-NETTY-SERVER-WORKER-THREADS 1
                    STORM-MESSAGING-NETTY-CLIENT-WORKER-THREADS 1
                    TOPOLOGY-KRYO-FACTORY "org.apache.storm.serialization.DefaultKryoFactory"
                    TOPOLOGY-TUPLE-SERIALIZER "org.apache.storm.serialization.types.ListDelegateSerializer"
                    TOPOLOGY-FALL-BACK-ON-JAVA-SERIALIZATION false
                    TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS false}
        storm-conf-sasl (assoc storm-conf
                                    STORM-MESSAGING-NETTY-AUTHENTICATION true
                                    TOPOLOGY-NAME "topo1-netty-sasl"
                                    STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD (str (Utils/secureRandomLong) ":" (Utils/secureRandomLong)))]
   (test-server-always-reconnects-fn storm-conf)          ;; test with sasl authentication disabled
   (test-server-always-reconnects-fn storm-conf-sasl)))   ;; test with sasl authentication enabled
