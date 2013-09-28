(ns backtype.storm.security.auth.SaslTransportPlugin-test
  (:use [clojure test])
  (import [backtype.storm.security.auth SaslTransportPlugin$User])
)

(deftest test-User-name
  (let [nam "Andy"
        user (SaslTransportPlugin$User. nam)]
    (are [a b] (= a b)
      nam (.toString user)
      (.getName user) (.toString user)
      (.hashCode nam) (.hashCode user)
    )
  )
)

(deftest test-User-equals
  (let [nam "Andy"
        user1 (SaslTransportPlugin$User. nam)
        user2 (SaslTransportPlugin$User. nam)
        user3 (SaslTransportPlugin$User. "Bobby")]
    (is (-> user1 (.equals user1)))
    (is (-> user1 (.equals user2)))
    (is (not (-> user1 (.equals nil))))
    (is (not (-> user1 (.equals "Potato"))))
    (is (not (-> user1 (.equals user3))))
  )
)
