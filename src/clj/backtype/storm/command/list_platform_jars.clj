(ns backtype.storm.command.list-platform-jars
  (:use [backtype.storm thrift])
  (:require [clojure.string :as string])
  (:gen-class))

(defn -main []
  (with-configured-nimbus-connection nimbus
    (let [jars (.listPlatformJars nimbus)]
      (if (empty? jars)
        (println "No platform jar.")
        (let [jars (string/split jars #":")]
          (doseq [jar jars]
            (println jar)))))))