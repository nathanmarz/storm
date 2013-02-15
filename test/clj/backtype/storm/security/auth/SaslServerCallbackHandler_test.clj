(ns backtype.storm.security.auth.SaslServerCallbackHandler-test
  (:use [clojure test])
  (:import [backtype.storm.security.auth SaslServerCallbackHandler]
           [javax.security.auth.login Configuration AppConfigurationEntry]
           [javax.security.auth.login AppConfigurationEntry$LoginModuleControlFlag]
           [javax.security.auth.callback NameCallback PasswordCallback]
           [javax.security.sasl AuthorizeCallback RealmCallback]
  )
)

(defn- mk-configuration-with-appconfig-mapping [mapping]
  ; The following defines a subclass of Configuration
  (proxy [Configuration] []
    (getAppConfigurationEntry [^String _]
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

(defn- handles-namecallback [handler username]
  (let [callback (new NameCallback "bogus prompt" username)]
    (-> handler (.handle (into-array [callback]))) ; side-effects on callback
    (is (= (.getDefaultName callback) (.getName callback))
      "Sets default name")
  )
)

(defn- handles-passwordcallback [handler expected]
  (let [callback (new PasswordCallback "bogus prompt" false)]
    (-> handler (.handle (into-array [callback]))) ; side-effects on callback
    (is (= expected (new String (.getPassword callback)))
      "Sets correct password when user credentials are present.")
  )
)

(defn- does-not-set-passwd-if-noname []
  (let [
        config (mk-configuration-with-appconfig-mapping {})
        handler (new SaslServerCallbackHandler config)
        callback (new PasswordCallback "bogus prompt" false)]
    (-> handler (.handle (into-array [callback]))) ; side-effects on callback
    (is (nil? (.getPassword callback))
      "Does not set password if no user name is set")
  )
)

(defn- handle-authorize-callback []
  (let [
        username "arbitraryUserName"
        password "arbitraryPassword"
        hostname "arbitraryHost"
        realm   "arbitraryDomain"
        id (str username "/" hostname "@" realm)
        callback (new AuthorizeCallback id id)
        callbackAry (into-array [callback])
        mapping {(str "user_" username) password}
        config (mk-configuration-with-appconfig-mapping mapping)
        handler (new SaslServerCallbackHandler config)
       ]

    ; Translate FOO/BAR@KAU -> FOO
    ; https://ccp.cloudera.com/display/CDH4DOC/Appendix+C+-+Configuring+the+Mapping+from+Kerberos+Principals+to+Short+Names
    ; This is so that KerberoseName member methods work, i.e. getShortName.
    (java.lang.System/setProperty
      "zookeeper.security.auth_to_local" "RULE:[2:$1]")

    ; Test kerberose short name case
    (java.lang.System/setProperty
      "storm.kerberos.removeHostFromPrincipal" "true")
    (java.lang.System/setProperty
      "storm.kerberos.removeRealmFromPrincipal" "true")
    (-> handler (.handle (into-array [callback]))) ; side-effects
    (is (.isAuthorized callback) "Returns true for isAuthorized")
    (is (= username (.getAuthorizedID callback))
        "Shortname is returned when removing host and realm")

    ; Let the host remain
    (java.lang.System/setProperty
      "storm.kerberos.removeHostFromPrincipal" "false")
    (-> callback (.setAuthorized false))
    (-> handler (.handle (into-array [callback]))) ; side-effects
    (is (.isAuthorized callback) "Returns true for isAuthorized")
    (is (= (str username "/" hostname) (.getAuthorizedID callback))
        "Returns shortname / host when removing realm" )

    ; Let the realm remain
    (java.lang.System/setProperty
      "storm.kerberos.removeHostFromPrincipal" "true")
    (java.lang.System/setProperty
      "storm.kerberos.removeRealmFromPrincipal" "false")
    (-> callback (.setAuthorized false))
    (-> handler (.handle (into-array [callback]))) ; side-effects
    (is (.isAuthorized callback) "Returns true for isAuthorized")
    (is (= (str username "@" realm) (.getAuthorizedID callback))
        "Returns shortname @ realm when removing host" )

    ; Let both the host and realm remain
    (java.lang.System/setProperty
      "storm.kerberos.removeHostFromPrincipal" "false")
    (java.lang.System/setProperty
      "storm.kerberos.removeHostFromPrincipal" "false")
    (-> callback (.setAuthorized false))
    (-> handler (.handle (into-array [callback]))) ; side-effects
    (is (.isAuthorized callback) "sets isAuthorized")
    (is (= (str username "/" hostname "@" realm) (.getAuthorizedID callback))
        "Returns shortname @ host / realm when not removing host or realm")
  )
)

(defn- handles-realm-callback [handler]
  (let [
        expected-default-text "the default text"
        callback (new RealmCallback "bogus prompt" expected-default-text)
       ]
    (-> handler (.handle (into-array [callback]))) ; side-effects on callback
    (is (= expected-default-text (.getText callback)) "Sets default realm")
  )
)

(deftest handle-sets-callback-fields-properly
  (let [
        username "Test User"
        expected-password "bogus password"
        mapping {(str "user_" username) expected-password}
        config (mk-configuration-with-appconfig-mapping mapping)
        handler (new SaslServerCallbackHandler config)
       ]
    (handles-namecallback handler username)
    (handles-passwordcallback handler expected-password)
    (handles-realm-callback handler)
    (does-not-set-passwd-if-noname)
    (handle-authorize-callback)
  )
)

(deftest handles-password-callback-for-super
  (let [
        username "super"
        expected-password "bogus password for super"
        mapping {(str "user_" username) expected-password}
        config (mk-configuration-with-appconfig-mapping mapping)
        handler (new SaslServerCallbackHandler config)
        name-callback (new NameCallback "bogus prompt" username)
        pass-callback (new PasswordCallback "bogus prompt" false)
       ]
    (java.lang.System/setProperty
      "storm.SASLAuthenticationProvider.superPassword" expected-password)
    (-> handler (.handle (into-array [name-callback]))) ; side-effects on name-callback
    (-> handler (.handle (into-array [pass-callback]))) ; side-effects on pass-callback
    (is (= expected-password (new String (.getPassword pass-callback)))
      "Sets correct password when user credentials are present.")

    ; Clean-up
    (java.lang.System/setProperty
      "storm.SASLAuthenticationProvider.superPassword" "")
  )
)

(deftest throws-on-null-appconfig
  (let [conf (mk-configuration-with-null-appconfig)]
    (is (thrown? java.io.IOException
      (new SaslServerCallbackHandler conf))
      "Throws IOException when no AppConfiguration is given"
    )
  )
)
