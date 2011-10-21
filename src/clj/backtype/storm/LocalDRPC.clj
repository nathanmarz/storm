(ns backtype.storm.LocalDRPC
  (:use [backtype.storm.daemon drpc])
  (:use [backtype.storm util])
  (:import [backtype.storm.utils InprocMessaging])
  (:gen-class
   :init init
   :implements [backtype.storm.generated.DistributedRPC$Iface backtype.storm.daemon.Shutdownable]
   :constructors {[backtype.storm.drpc.SpoutAdder] []}
   :state state ))

(defn -init [adder]
  (let [port (InprocMessaging/acquireNewPort)
        handler (service-handler adder port)
        port-thread (async-loop (fn []
                                  (let [[id result] (InprocMessaging/takeMessage port)]
                                    (.result handler id result))
                                  0 )
                                :daemon true)
        ]
    [[] {:thread port-thread :handler handler}]
    ))

(defn -execute [this func funcArgs]
  (.execute (:handler (. this state)) func funcArgs)
  )

(defn -result [this id result]
  (.result (:handler (. this state)) id result)
  )
  
(defn -shutdown [this]
  (.interrupt {:thread (. this state)})
  )
