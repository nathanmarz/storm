(ns backtype.storm.command.delete-platform-jar
  (:use [backtype.storm thrift])
  (require [clojure.string :as string])
  (:gen-class))

(defn -main [& jarfiles]
  (with-configured-nimbus-connection nimbus
    (doseq [jarfile jarfiles
            :let [jarfile (string/trim jarfile)]]
      (.deletePlatformJar nimbus jarfile)
      (println jarfile " DELETED."))))