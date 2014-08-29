(ns backtype.storm.security.auth.DefaultHttpCredentialsPlugin-test
  (:use [clojure test])
  (:import [javax.security.auth Subject])
  (:import [javax.servlet.http HttpServletRequest])
  (:import [backtype.storm.security.auth SingleUserPrincipal])
  (:import [org.mockito Mockito])
  (:import [backtype.storm.security.auth DefaultHttpCredentialsPlugin
            ReqContext SingleUserPrincipal])
  )

(deftest test-getUserName
  (let [handler (doto (DefaultHttpCredentialsPlugin.) (.prepare {}))]
    (testing "returns null when request is null"
      (is (nil? (.getUserName handler nil))))

    (testing "returns null when user principal is null"
      (let [req (Mockito/mock HttpServletRequest)]
        (is (nil? (.getUserName handler req)))))

    (testing "returns null when user is blank"
      (let [princ (SingleUserPrincipal. "")
            req (Mockito/mock HttpServletRequest)]
        (. (Mockito/when (. req getUserPrincipal))
           thenReturn princ)
        (is (nil? (.getUserName handler req)))))

    (testing "returns correct user from requests principal"
      (let [exp-name "Alice"
            princ (SingleUserPrincipal. exp-name)
            req (Mockito/mock HttpServletRequest)]
        (. (Mockito/when (. req getUserPrincipal))
           thenReturn princ)
        (is (.equals exp-name (.getUserName handler req)))))))

(deftest test-populate-req-context-on-null-user
  (let [req (Mockito/mock HttpServletRequest)
        handler (doto (DefaultHttpCredentialsPlugin.) (.prepare {}))
        subj (Subject. false (set [(SingleUserPrincipal. "test")]) (set []) (set []))
        context (ReqContext. subj)]
    (is (= 0 (-> handler (.populateContext context req) (.subject) (.getPrincipals) (.size))))))
