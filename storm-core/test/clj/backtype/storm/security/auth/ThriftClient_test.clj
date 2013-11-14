(ns backtype.storm.security.auth.ThriftClient-test
  (:use [backtype.storm config])
  (:use [clojure test])
  (:import [backtype.storm.security.auth ThriftClient])
  (:import [org.apache.thrift7.transport TTransportException])
)

(deftest test-ctor-throws-if-port-invalid
  (let [conf (read-default-config)
        timeout (Integer. 30)]
    (is (thrown? java.lang.IllegalArgumentException
      (ThriftClient. conf "bogushost" -1 timeout)))
    (is (thrown? java.lang.IllegalArgumentException
        (ThriftClient. conf "bogushost" 0 timeout)))
  )
)

(deftest test-ctor-throws-if-host-not-set
  (let [conf (read-default-config)
        timeout (Integer. 60)]
    (is (thrown? TTransportException
         (ThriftClient. conf "" 4242 timeout)))
    (is (thrown? IllegalArgumentException
        (ThriftClient. conf nil 4242 timeout)))
  )
)
