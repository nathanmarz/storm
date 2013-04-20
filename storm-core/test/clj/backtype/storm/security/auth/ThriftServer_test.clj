(ns backtype.storm.security.auth.ThriftServer-test
  (:use [backtype.storm config])
  (:use [clojure test])
  (:import [backtype.storm.security.auth ThriftServer])
  (:import [org.apache.thrift7.transport TTransportException])
)

(deftest test-stop-checks-for-null
  (let [server (ThriftServer. (read-default-config) nil 12345)]
    (.stop server)))

(deftest test-isServing-checks-for-null
  (let [server (ThriftServer. (read-default-config) nil 12345)]
    (is (not (.isServing server)))))
