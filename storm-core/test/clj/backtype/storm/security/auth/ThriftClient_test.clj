(ns backtype.storm.security.auth.ThriftClient-test
  (:use [backtype.storm config util])
  (:use [clojure test])
  (:import [backtype.storm.security.auth ThriftClient])
  (:import [org.apache.thrift7.transport TTransportException])
)

(deftest test-ctor-throws-if-port-invalid
  (let [conf (merge
              (read-default-config)
              {STORM-NIMBUS-RETRY-TIMES 0})
        timeout (Integer. 30)]
    (is (thrown-cause? java.lang.IllegalArgumentException
      (ThriftClient. conf "bogushost" -1 timeout)))
    (is (thrown-cause? java.lang.IllegalArgumentException
        (ThriftClient. conf "bogushost" 0 timeout)))
  )
)

(deftest test-ctor-throws-if-host-not-set
  (let [conf (merge
              (read-default-config)
              {STORM-NIMBUS-RETRY-TIMES 0})
        timeout (Integer. 60)]
    (is (thrown-cause? TTransportException
         (ThriftClient. conf "" 4242 timeout)))
    (is (thrown-cause? IllegalArgumentException
        (ThriftClient. conf nil 4242 timeout)))
  )
)
