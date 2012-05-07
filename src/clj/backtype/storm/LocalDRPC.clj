(ns backtype.storm.LocalDRPC
  (:use [backtype.storm.daemon drpc])
  (:use [backtype.storm util])
  (:import [backtype.storm.utils InprocMessaging ServiceRegistry])
  (:gen-class
   :init init
   :implements [backtype.storm.ILocalDRPC]
   :constructors {[] []}
   :state state ))

(defn -init []
  (let [handler (service-handler)
        id (ServiceRegistry/registerService handler)
        ]
    [[] {:service-id id :handler handler}]
    ))

(defn -executeBinary [this func funcArgs]
  (.executeBinary (:handler (. this state)) func funcArgs)
  )

(defn -execute [this func funcArgs]
  (.execute (:handler (. this state)) func funcArgs)
  )

(defn -result [this id result]
  (.result (:handler (. this state)) id result)
  )

(defn -fetchRequest [this func]
  (.fetchRequest (:handler (. this state)) func)
  )

(defn -failRequest [this id]
  (.failRequest (:handler (. this state)) id)
  )
  
(defn -getServiceId [this]
  (:service-id (. this state)))  

(defn -shutdown [this]
  (ServiceRegistry/unregisterService (:service-id (. this state)))
  (.shutdown (:handler (. this state)))
  )
