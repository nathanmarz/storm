(ns backtype.storm.log
  (:require [clojure.tools [logging :as log]]))

(defmacro log-message [& args]
  `(log/info (str ~@args)))

(defmacro log-error [e & args]
  `(log/error (str ~@args) ~e))

(defmacro log-debug [& args]
  `(log/debug (str ~@args)))
