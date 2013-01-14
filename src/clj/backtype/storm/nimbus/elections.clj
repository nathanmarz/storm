(ns backtype.storm.nimbus.elections
  (:import [backtype.storm.nimbus NimbusLeaderElections])
  (:use [backtype.storm config util log]))

(defn local-hostname-conf [conf]
  (if (contains? conf STORM-LOCAL-HOSTNAME)
    (conf STORM-LOCAL-HOSTNAME)
    (local-hostname)))

(defn get-nimbus-leader-addr [conf]
  (let [leader-elections (NimbusLeaderElections.)]
    (.init leader-elections conf nil)
    (let [leader-addr (.getLeaderAddr leader-elections)]
      (.close leader-elections)
      leader-addr)))

(defn get-nimbus-addr-list [conf]
  (let [leader-elections (NimbusLeaderElections.)]
    (.init leader-elections conf nil)
    (let [addr-list (.getNimbusList leader-elections)]
      (.close leader-elections)
      addr-list)))

(defn await-leadership [conf storage]
  (let [leader-elections (NimbusLeaderElections.)]
    (when-let [leader-addr (get-nimbus-leader-addr conf)]
      (log-message "Current Nimbus leader: " leader-addr)
      (if-not (.isSupportDistributed storage)
        (throw (IllegalStateException. "Trying to start secondary Nimbus with storage that isn't support distributed"))))
    (.init leader-elections conf (str (local-hostname-conf conf) ":" (conf NIMBUS-THRIFT-PORT)))
    (log-message "Nimbus awaiting for leadership")
    (.awaitLeadership leader-elections)
    (log-message "Nimbus gained leadership")
    leader-elections))

(defn ensure-leadership [leader-elections]
  (when-not (.hasLeadership leader-elections) (throw (backtype.storm.generated.NotALeaderException.))))
