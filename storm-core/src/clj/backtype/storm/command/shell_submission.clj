(ns backtype.storm.command.shell-submission
  (:import [backtype.storm StormSubmitter])
  (:use [backtype.storm thrift util config log])
  (:require [clojure.string :as str])
  (:gen-class))


(defn -main [^String tmpjarpath & args]
  (let [conf (read-storm-config)
        host (conf NIMBUS-HOST)
        port (conf NIMBUS-THRIFT-PORT)
        jarpath (StormSubmitter/submitJar conf tmpjarpath)
        args (concat args [host port jarpath])]
    (exec-command! (str/join " " args))
    ))
