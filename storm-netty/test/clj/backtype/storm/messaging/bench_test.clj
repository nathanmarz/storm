(ns backtype.storm.messaging.bench-test
  (:use [clojure test])
  (:import [backtype.storm.messaging TransportFactory])
  (:use [backtype.storm bootstrap testing util]))

(bootstrap)

(def port 6800) 
(def task 1) 
(def msg_count 100000)
(def storm-id "abc")
(def buffer_size 102400)
(def repeats 1000)

(defn batch-bench [client server] 
  (doseq [num  (range 1 msg_count)]
    (let [req_msg (str num)]
      (.send client task (.getBytes req_msg))))
  (doseq [num  (range 1 msg_count)]
    (let [req_msg (str num)]
      (.recv server 0))))

(defn one-by-one-bench [client server]
  (let [req_msg (apply str (repeat buffer_size 'c'))]
    (doseq [num  (range 1 repeats)]
      (.send client task (.getBytes req_msg))
      (.recv server 0))))    

(deftest test-netty-perf
  (let [storm-conf {STORM-MESSAGING-TRANSPORT "backtype.storm.messaging.netty.Context"
                    STORM-MESSAGING-NETTY-BUFFER-SIZE buffer_size
                    STORM-MESSAGING-NETTY-MAX-RETRIES 10
                    STORM-MESSAGING-NETTY-MIN-SLEEP-MS 1000 
                    STORM-MESSAGING-NETTY-MAX-SLEEP-MS 5000}
        context (TransportFactory/makeContext storm-conf)
        server (.bind context nil port)
        client (.connect context nil "localhost" port)]
    (log-message "(Netty) " repeats " messages of payload size " buffer_size " sent one by one")
    (time (one-by-one-bench client server))
    (log-message "(Netty) " msg_count " short msgs in batches" )
    (time (batch-bench client server))
    (.close client)
    (.close server)
    (.term context)))

(deftest test-zmq-perf
  (let [storm-conf (merge (read-storm-config)
                          {STORM-MESSAGING-TRANSPORT  "backtype.storm.messaging.zmq"
                           "topology.executor.receive.buffer.size" buffer_size
                           "topology.executor.send.buffer.size" buffer_size
                           "topology.receiver.buffer.size" buffer_size
                           "topology.transfer.buffer.size" buffer_size})
        context (TransportFactory/makeContext storm-conf)
        server (.bind context nil port)
        client (.connect context nil "localhost" port)]
    (log-message "(ZMQ) " repeats " messages of payload size " buffer_size " sent one by one")
    (time (one-by-one-bench client server))
    (log-message "(ZMQ) " msg_count " short msgs in batches" )
    (time (batch-bench client server))
    (.close client)
    (.close server)
    (.term context)))
