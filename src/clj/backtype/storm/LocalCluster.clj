(ns backtype.storm.LocalCluster
  (:use [backtype.storm testing])
  (:import [backtype.storm StormSubmitter])
  (:gen-class
   :init init
   :implements [backtype.storm.ILocalCluster]
   :constructors {[] []}
   :state state ))

(defn -init []
  (let [ret (mk-local-storm-cluster)]
    [[] ret]
    ))

(defn -submitTopology [this name conf topology]
  ; validate the config first
  (StormSubmitter/validateStormConf conf)

  (submit-local-topology (:nimbus (. this state))
                      name
                      conf
                      topology))

(defn -shutdown [this]
  (kill-local-storm-cluster (. this state))
  )

(defn -killTopology [this name]
  (.killTopology (:nimbus (. this state)) name)
  )
