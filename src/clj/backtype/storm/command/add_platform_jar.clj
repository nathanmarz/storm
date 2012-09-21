(ns backtype.storm.command.add-platform-jar
  (:use [backtype.storm config])
  (:require [clojure.string :as string])
  (:import [backtype.storm StormSubmitter])
  (:gen-class))

(defn -main [& jarfiles]
  (let [config (read-storm-config)]
    (doseq [jarfile jarfiles
            :let [jarfile (string/trim jarfile)]]
      (StormSubmitter/addPlatformJar config jarfile)
      (println jarfile " UPLOADED."))))