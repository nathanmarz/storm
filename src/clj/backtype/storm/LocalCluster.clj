(ns backtype.storm.LocalCluster
  (:use [backtype.storm testing config])
  (:gen-class
   :init init
   :implements [backtype.storm.ILocalCluster]
   :constructors {[] []}
   :state state ))

(defn -init []
  (let [ret (mk-local-storm-cluster :daemon-conf {TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS true})]
    [[] ret]
    ))

(defn -submitTopology [this name conf topology]
  (submit-local-topology (:nimbus (. this state))
                      name
                      conf
                      topology))

(defn -shutdown [this]
  (kill-local-storm-cluster (. this state)))

(defn -killTopology [this name]
  (.killTopology (:nimbus (. this state)) name))

(defn -getTopologyConf [this id]
  (.getTopologyConf (:nimbus (. this state)) id))

(defn -getTopology [this id]
  (.getTopology (:nimbus (. this state)) id))

(defn -getClusterInfo [this]
  (.getClusterInfo (:nimbus (. this state))))

(defn -getTopologyInfo [this id]
  (.getTopologyInfo (:nimbus (. this state)) id))

(defn -killTopologyWithOpts [this name opts]
  (.killTopologyWithOpts (:nimbus (. this state)) name opts))

(defn -activate [this name]
  (.activate (:nimbus (. this state)) name))

(defn -deactivate [this name]
  (.deactivate (:nimbus (. this state)) name))

(defn -rebalance [this name opts]
  (.rebalance (:nimbus (. this state)) name opts))
