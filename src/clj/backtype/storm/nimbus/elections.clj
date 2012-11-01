(ns backtype.storm.nimbus.elections
  (:import [backtype.storm.nimbus NimbusLeaderElections])
  (:use [backtype.storm config util]))

(defn local-hostname-conf [conf]
  (if (contains? conf STORM-LOCAL-HOSTNAME)
    (conf STORM-LOCAL-HOSTNAME)
    (local-hostname)))

(defn get-nimbus-leader-host [conf]
  (let [leader-elections (NimbusLeaderElections.)]
    (.init leader-elections conf nil)
    (.getLeaderId leader-elections)))

(defn await-leadership [conf]
  (let [leader-elections (NimbusLeaderElections.)]
    (.init leader-elections conf (local-hostname-conf conf))
    (.awaitLeadership leader-elections)
    leader-elections))

(defn ensureLeadership [leader-elections]
  ;; todo replace RE with some specific exception, supported by Nimbus thrift service
  (when-not (.hasLeadership leader-elections) (throw (RuntimeException. "Not a leader"))))
