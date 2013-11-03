(ns backtype.storm.daemon.logviewer
  (:use compojure.core)
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util log])
  (:use [ring.adapter.jetty :only [run-jetty]])
  (:import [org.apache.commons.logging LogFactory])
  (:import [org.apache.commons.logging.impl Log4JLogger])
  (:import [org.apache.log4j Level])
  (:require [compojure.route :as route]
            [compojure.handler :as handler])
  (:gen-class))

(defn tail-file [path tail]
  (let [flen (.length (clojure.java.io/file path))
        skip (- flen tail)]
    (with-open [input (clojure.java.io/input-stream path)
                output (java.io.ByteArrayOutputStream.)]
      (if (> skip 0) (.skip input skip))
      (let [buffer (make-array Byte/TYPE 1024)]
        (loop []
          (let [size (.read input buffer)]
            (when (and (pos? size) (< (.size output) tail))
              (do (.write output buffer 0 size)
                  (recur))))))
      (.toString output))
    ))

(defn log-page [file tail grep]
  (let [path (str (System/getProperty "storm.home") "/logs/" file)
        tail (if tail
               (min 10485760 (Integer/parseInt tail))
               10240)
        tail-string (tail-file path tail)]
    (if grep
       (clojure.string/join "\n<br>"
         (filter #(.contains % grep) (.split tail-string "\n")))
       (.replaceAll tail-string "\n" "\n<br>"))))

(defn log-level-page [name level]
  (let [log (LogFactory/getLog name)]
    (if level
      (if (instance? Log4JLogger log)
        (.setLevel (.getLogger log) (Level/toLevel level))))
    (str "effective log level for " name " is " (.getLevel (.getLogger log)))))

(defn log-template [body]
  (html4
   [:head
    [:title "Storm log viewer"]
    (include-css "/css/bootstrap-1.1.0.css")
    (include-css "/css/style.css")
    (include-js "/js/jquery-1.6.2.min.js")
    (include-js "/js/jquery.tablesorter.min.js")
    (include-js "/js/jquery.cookies.2.2.0.min.js")
    (include-js "/js/script.js")
    ]
   [:body
    (seq body)
    ]))

(defroutes log-routes
  (GET "/log" [:as {cookies :cookies} & m]
       (log-template (log-page (:file m) (:tail m) (:grep m))))
  (GET "/loglevel" [:as {cookies :cookies} & m]
       (log-template (log-level-page (:name m) (:level m))))
  (route/resources "/")
  (route/not-found "Page not found"))

(def logapp
  (handler/site log-routes)
 )

(defn start-logviewer [port]
  (run-jetty logapp {:port port}))

(defn -main []
  (let [conf (read-storm-config)]
    (start-logviewer (int (conf LOGVIEWER-PORT)))))

