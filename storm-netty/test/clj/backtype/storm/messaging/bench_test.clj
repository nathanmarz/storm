(ns backtype.storm.messaging.bench-test
  (:use [clojure test])
  (:import [backtype.storm.messaging TransportFactory])
  (:use [backtype.storm bootstrap testing util]))

(bootstrap)

(def port 6700) 
(def task 1) 
(def msg_count 100000)

(deftest test-netty-perf
  (time (let [storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-BUFFER-SIZE 1024000
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000 
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000}
        context (TransportFactory/makeContext storm-conf)
        server (.bind context nil port)
        client (.connect context nil "localhost" port)]
    (doseq [num  (range 1 msg_count)]
      (let [req_msg (str num)]
        (.send client task (.getBytes req_msg))))
    (doseq [num  (range 1 msg_count)]
      (let [req_msg (str num)
            resp (.recv server 0)
            resp_msg (String. (.message resp))]
        (is (= req_msg resp_msg))))
    (.close client)
    (.close server)
    (.term context))))

(deftest test-zmq-perf
  (time (let [storm-conf (merge (read-storm-config)
                          {STORM-MESSAGING-TRANSPORT  "backtype.storm.messaging.zmq"})
        context (TransportFactory/makeContext storm-conf)
        server (.bind context nil port)
        client (.connect context nil "localhost" port)]
    (doseq [num  (range 1 msg_count)]
      (let [req_msg (str num)]
        (.send client task (.getBytes req_msg))))
    (doseq [num  (range 1 msg_count)]
      (let [req_msg (str num)
            resp (.recv server 0)
            resp_msg (String. (.message resp))]
        (is (= req_msg resp_msg))))
    (.close client)
    (.close server)
    (.term context))))
