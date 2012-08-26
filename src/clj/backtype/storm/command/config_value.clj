(ns backtype.storm.command.config-value
  (:use [backtype.storm config log])
  (:gen-class))

(defn -main
  ([^String name]
    (let [conf (read-storm-config)]
      (println "VALUE:" (conf name))
      ))
  ([^String name ^String file]
    (let [conf (read-storm-config file)]
      (println "VALUE:" (conf name))
      )))

