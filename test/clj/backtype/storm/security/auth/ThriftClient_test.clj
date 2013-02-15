(ns backtype.storm.security.auth.ThriftClient-test
  (:use [clojure test])
  (:import [backtype.storm.security.auth ThriftClient])
)

; Exceptions are getting wrapped in RuntimeException.  This might be due to
; CLJ-855.
(defn- unpack-runtime-exception [expression]
  (try (eval expression)
    nil
    (catch java.lang.RuntimeException gripe
      (throw (.getCause gripe)))
  )
)

(deftest test-ctor-throws-if-port-invalid
  (is (thrown? java.lang.IllegalArgumentException
    (unpack-runtime-exception 
      '(ThriftClient. "bogushost" -1 "Fake Service Name"))))
  (is
    (thrown? java.lang.IllegalArgumentException
      (unpack-runtime-exception
        '(ThriftClient. "bogushost" 0 "Fake Service Name"))))
)

(deftest test-ctor-throws-if-host-not-set
  (is
    (thrown? IllegalArgumentException
      (unpack-runtime-exception
        '(ThriftClient. "" 4242 "Fake Service Name"))))
  (is
    (thrown? IllegalArgumentException
      (unpack-runtime-exception
        '(ThriftClient. nil 4242 "Fake Service Name"))))
)
