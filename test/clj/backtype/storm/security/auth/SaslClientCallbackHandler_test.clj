(ns backtype.storm.security.auth.SaslClientCallbackHandler-test
  (:use [clojure test])
  (:import [backtype.storm.security.auth SaslClientCallbackHandler]
           [javax.security.auth.login Configuration AppConfigurationEntry]
           [javax.security.auth.login AppConfigurationEntry$LoginModuleControlFlag]
           [javax.security.auth.callback NameCallback PasswordCallback]
           [javax.security.sasl AuthorizeCallback RealmCallback]
  )
)

(defn- mk-configuration-with-appconfig-mapping [mapping]
  ; The following defines a subclass of Configuration
  (proxy [Configuration] []
    (getAppConfigurationEntry [^String nam]
      (into-array [(new AppConfigurationEntry "bogusLoginModuleName"
         AppConfigurationEntry$LoginModuleControlFlag/REQUIRED
         mapping
      )])
    )
  )
)

(defn- mk-configuration-with-null-appconfig []
  ; The following defines a subclass of Configuration
  (proxy [Configuration] []
    (getAppConfigurationEntry [^String nam] nil)
  )
)

(defn- handles-namecallback [handler expected]
  (let [callback (new NameCallback "bogus prompt" "not right")]
    (-> handler (.handle (into-array [callback]))) ; side-effects on callback
    (is (= expected (.getName callback))
      "Sets correct name")
  )
)

(defn- handles-passwordcallback [handler expected]
  (let [callback (new PasswordCallback "bogus prompt" false)]
    (-> handler (.handle (into-array [callback]))) ; side-effects on callback
    (is (= expected (new String (.getPassword callback)))
      "Sets correct password when user credentials are present.")
  )
)

(defn- handles-authorized-callback [handler]
  (let [
         id "an ID"
         callback
           (new AuthorizeCallback id id)
         another-id "bogus authorization ID"
         callback2
           (new AuthorizeCallback id another-id)
       ]
    (-> handler (.handle (into-array [callback]))) ; side-effects on callback
    (is (.isAuthorized callback) "isAuthorized is true for valid IDs")
    (is (= id (.getAuthorizedID callback)) "Authorized ID properly set")

    (-> handler (.handle (into-array [callback2]))) ; side-effects on callback
    (is (not (.isAuthorized callback2)) "isAuthorized is false for differing IDs")
    (is (not (= another-id (.getAuthorizedID callback2))) "Authorized ID properly set")
  )
)

(defn- handles-realm-callback [handler]
  (let [
        expected-default-text "the default text"
        callback (new RealmCallback "bogus prompt" expected-default-text)
       ]
    (-> handler (.handle (into-array [callback]))) ; side-effects on callback
    (is (= expected-default-text (.getText callback))
        "Returns expected default realm text")
  )
)

(deftest handle-sets-callback-fields-properly
  (let [
        expected-username "Test User"
        expected-password "a really lame password"
        config (mk-configuration-with-appconfig-mapping
                 {"username" expected-username
                  "password" expected-password})
        handler (new SaslClientCallbackHandler config)
       ]
    (handles-namecallback handler expected-username)
    (handles-passwordcallback handler expected-password)
    (handles-authorized-callback handler)
    (handles-realm-callback handler)
  )
)

(deftest throws-on-null-appconfig
  (let [conf (mk-configuration-with-null-appconfig)]
    (is (thrown? java.io.IOException
      (new SaslClientCallbackHandler conf))
      "Throws IOException when no AppConfiguration is given"
    )
  )
)
