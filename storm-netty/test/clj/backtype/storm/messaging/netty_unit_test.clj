(ns backtype.storm.messaging.netty-unit-test
  (:use [clojure test])
  (:import [backtype.storm.messaging TransportFactory])
  (:use [backtype.storm bootstrap testing util]))

(bootstrap)

(deftest test-basic
  (let [req_msg (String. "0123456789abcdefghijklmnopqrstuvwxyz")
        storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000 
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000 }
        context (TransportFactory/makeContext storm-conf)
        port 6700
        server (.bind context nil port)
        client (.connect context nil "localhost" port)
        task 0
        _ (.send client task (.getBytes req_msg))
        resp (.recv server 0)]
    (is (= task (.task resp)))
    (is (= req_msg (String. (.message resp))))
    (.term context)))    

(deftest test-large-msg
  (let [req_msg (apply str (repeat 2048000 'c')) 
        storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 102400
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000 
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000 }
        context (TransportFactory/makeContext storm-conf)
        port 6701
        server (.bind context nil port)
        client (.connect context nil "localhost" port)
        task 0
        _ (.send client task (.getBytes req_msg))
        resp (.recv server 0)]
    (is (= task (.task resp)))
    (is (= req_msg (String. (.message resp))))
    (.term context)))    
    
(deftest test-server-delayed
    (let [req_msg (String. "0123456789abcdefghijklmnopqrstuvwxyz")
       storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000 
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000 }
        context (TransportFactory/makeContext storm-conf)
        port 6702
        client (.connect context nil "localhost" port)
        task 0
        _ (.send client task (.getBytes req_msg))
        _ (Thread/sleep 2000)
        server (.bind context nil port)
        resp (.recv server 0)]
    (is (= task (.task resp)))
    (is (= req_msg (String. (.message resp))))
    (.term context)))    

(deftest test-batch
  (let [storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024000
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000 
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000}
        context (TransportFactory/makeContext storm-conf)
        port 6703
        server (.bind context nil port)
        client (.connect context nil "localhost" port)
        task 0]
    (doseq [num  (range 1 100000)]
      (let [req_msg (str num)]
        (.send client task (.getBytes req_msg))))
    (doseq [num  (range 1 100000)]
      (let [req_msg (str num)
            resp (.recv server 0)
            resp_msg (String. (.message resp))]
        (is (= req_msg resp_msg))))
    (.term context)))
