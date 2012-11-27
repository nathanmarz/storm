(ns backtype.storm.nimbus.elections
  (:import [backtype.storm.nimbus NimbusLeaderElections])
  (:use [backtype.storm config util log]))

(defn local-hostname-conf [conf]
  (if (contains? conf STORM-LOCAL-HOSTNAME)
    (conf STORM-LOCAL-HOSTNAME)
    (local-hostname)))

(defn get-nimbus-leader-host [conf]
  (let [leader-elections (NimbusLeaderElections.)]
    (.init leader-elections conf nil)
    (let [leader-id (.getLeaderId leader-elections)]
      (.close leader-elections)
      leader-id)))

(defn await-leadership [conf storage]
  (let [leader-elections (NimbusLeaderElections.)]
    (when-let [leader-id (get-nimbus-leader-host conf)]
      (log-message "Current Nimbus leader: " leader-id)
      (if-not (.isSupportDistributed storage)
        (throw (IllegalStateException. "Trying to start secondary Nimbus with storage that isn't support distributed"))))
    (.init leader-elections conf (local-hostname-conf conf))
    (log-message "Nimbus awaiting for leadership")
    (.awaitLeadership leader-elections)
    (log-message "Nimbus gained leadership")
    leader-elections))

(defn ensure-leadership [leader-elections]
  (when-not (.hasLeadership leader-elections) (throw (backtype.storm.generated.NotALeaderException.))))
