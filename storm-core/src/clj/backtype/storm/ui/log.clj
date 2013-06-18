(ns backtype.storm.ui.log
  (:use compojure.core)
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util])
  (:use [backtype.storm.ui helpers core])
  (:use [backtype.storm.daemon [common :only [ACKER-COMPONENT-ID system-id?]]])
  (:use [ring.adapter.jetty :only [run-jetty]])
  (:use [clojure.string :only [trim]])
  (:import [java.io File])
  (:import [org.apache.commons.logging LogFactory])
  (:import [org.apache.commons.logging.impl Log4JLogger])
  (:import [org.apache.log4j Level])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [backtype.storm [thrift :as thrift]])
  (:gen-class))

(defn log-page [file tail lines grep]
  (let [path (str (System/getProperty "storm.home") "/logs/" file)
        flen (.length (clojure.java.io/file path))
        nline (if lines (min 10000 (Integer/parseInt lines)) 1000)
        skip (- flen (if tail (min 10485760 (Integer/parseInt tail)) 10240))]
    (with-open [rdr (clojure.java.io/reader path)]
      (if (> skip 0) (.skip rdr skip))
      (if grep 
          (clojure.string/join "\n<br>" (take nline (filter #(.contains % grep) (line-seq rdr))))
          (clojure.string/join "\n<br>" (take nline (line-seq rdr)))))))

(defn log-level-page [name level]
  (let [log (LogFactory/getLog name)]
    (if level
      (if (instance? Log4JLogger log) 
        (.setLevel (.getLogger log) (Level/toLevel level))))
    (str "effective log level for " name " is " (.getLevel (.getLogger log)))))

(defroutes log-routes
  (GET "/log" [:as {cookies :cookies} & m]
       (ui-template (log-page (:file m) (:tail m) (:lines m) (:grep m))))
  (GET "/loglevel" [:as {cookies :cookies} & m]
       (ui-template (log-level-page (:name m) (:level m))))
  (route/resources "/")
  (route/not-found "Page not found"))

(def logapp
  (handler/site log-routes)
 )

(defn start-log-ui [port]
  (run-jetty logapp {:port port :join? false}))

