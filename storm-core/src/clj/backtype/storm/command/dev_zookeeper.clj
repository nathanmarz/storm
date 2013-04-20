(ns backtype.storm.command.dev-zookeeper
  (:use [backtype.storm zookeeper util config])
  (:gen-class))

(defn -main [& args]
  (let [conf (read-storm-config)
        port (conf STORM-ZOOKEEPER-PORT)
        localpath (conf DEV-ZOOKEEPER-PATH)]
    (rmr localpath)
    (mk-inprocess-zookeeper localpath :port port)
    ))
