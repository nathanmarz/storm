(ns backtype.storm.security.auth.AuthUtils-test
  (:import [backtype.storm.security.auth AuthUtils])
  (:import [java.io IOException])
  (:import [javax.security.auth.login AppConfigurationEntry Configuration])
  (:import [org.mockito Mockito])
  (:use [clojure test])
)

(deftest test-throws-on-missing-section
  (is (thrown? IOException
    (AuthUtils/get (Mockito/mock Configuration) "bogus-section" "")))
)

(defn- mk-mock-app-config-entry []
  (let [toRet (Mockito/mock AppConfigurationEntry)]
    (. (Mockito/when (.getOptions toRet)) thenReturn (hash-map))
    toRet
  )
)

(deftest test-returns-null-if-no-such-section
  (let [entry (mk-mock-app-config-entry)
        entries (into-array (.getClass entry) [entry])
        section "bogus-section"
        conf (Mockito/mock Configuration)]
    (. (Mockito/when (. conf getAppConfigurationEntry section ))
       thenReturn entries)
    (is (nil? (AuthUtils/get conf section "nonexistent-key")))
  )
)

(deftest test-returns-first-value-for-valid-key
  (let [k "the-key"
        expected "good-value"
        empty-entry (mk-mock-app-config-entry)
        bad-entry (Mockito/mock AppConfigurationEntry)
        good-entry (Mockito/mock AppConfigurationEntry)
        conf (Mockito/mock Configuration)]
    (. (Mockito/when (.getOptions bad-entry)) thenReturn {k "bad-value"})
    (. (Mockito/when (.getOptions good-entry)) thenReturn {k expected})
    (let [entries (into-array (.getClass empty-entry)
                    [empty-entry good-entry bad-entry])
          section "bogus-section"]
      (. (Mockito/when (. conf getAppConfigurationEntry section))
         thenReturn entries)
      (is (not (nil? (AuthUtils/get conf section k))))
      (is (= (AuthUtils/get conf section k) expected))
    )
  )
)
