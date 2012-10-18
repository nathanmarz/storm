(ns backtype.storm.command.config-value
  (:use [backtype.storm config log])
  (:gen-class))


(defn -main [^String name]
  (let [conf (read-storm-config)]
    (println "VALUE:" (conf name))
    ))
