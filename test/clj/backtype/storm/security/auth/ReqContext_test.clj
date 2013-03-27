(ns backtype.storm.security.auth.ReqContext-test
  (:import [backtype.storm.security.auth ReqContext])
  (:import [java.net InetAddress])
  (:import [java.security AccessControlContext Principal])
  (:import [javax.security.auth Subject])
  (:use [clojure test])
)

(def test-subject
  (let [rc (ReqContext/context)
        expected (Subject.)]
    (is (not (.isReadOnly expected)))
    (.setSubject rc expected)
    (is (= (.subject rc) expected))

    ; Change the Subject by setting read-only.
    (.setReadOnly expected)
    (.setSubject rc expected)
    (is (= (.subject rc) expected))
  )
)

(deftest test-remote-address
  (let [rc (ReqContext/context)
        expected (InetAddress/getByAddress (.getBytes "ABCD"))]
    (.setRemoteAddress rc expected)
    (is (= (.remoteAddress rc) expected))
  )
)

(deftest test-principal-returns-null-when-no-subject
  (let [rc (ReqContext/context)]
    (.setSubject rc (Subject.))
    (is (nil? (.principal rc)))
  )
)

(def principal-name "Test Principal")

(defn TestPrincipal []
  (reify Principal
    (^String getName [this]
      principal-name)
  )
)

(deftest test-principal
  (let [p (TestPrincipal)
        principals (hash-set p)
        creds (hash-set)
        s (Subject. false principals creds, creds)
        rc (ReqContext/context)]
    (.setSubject rc s)
    (is (not (nil? (.principal rc))))
    (is (= (-> rc .principal .getName) principal-name))
  )
)
