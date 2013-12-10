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
        resp (.recv server 0)]
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
        resp (.recv server 0)]
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
        _ (.send client task (.getBytes req_msg))
        _ (Thread/sleep 1000)
        server (.bind context nil port)
        resp (.recv server 0)]
    (is (= task (.task resp)))
    (is (= req_msg (String. (.message resp))))
    (.close client)
    (.close server)
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
    (doseq [num  (range 1 100000)]
      (let [req_msg (str num)
            resp (.recv server 0)
            resp_msg (String. (.message resp))]
        (is (= req_msg resp_msg))))
    (.close client)
    (.close server)
    (.term context)))
