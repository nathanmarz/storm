;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns org.apache.storm.daemon.drpc
  (:import [org.apache.storm.security.auth AuthUtils ReqContext]
           [org.apache.storm.daemon DrpcServer]
           [org.apache.storm.metric StormMetricsRegistry])
  (:import [org.apache.storm.utils Utils])
  (:import [org.apache.storm.utils ConfigUtils])
  (:use [org.apache.storm config log util])
  (:use [org.apache.storm.ui helpers])
  (:use compojure.core)
  (:use ring.middleware.reload)
  (:require [compojure.handler :as handler])
  (:gen-class))

(def drpc:num-execute-http-requests (StormMetricsRegistry/registerMeter "drpc:num-execute-http-requests"))

(defn handle-request [handler]
  (fn [request]
    (handler request)))

(defn populate-context!
  "Populate the Storm RequestContext from an servlet-request. This should be called in each handler"
  [http-creds-handler servlet-request]
    (when http-creds-handler
      (.populateContext http-creds-handler (ReqContext/context) servlet-request)))

(defn webapp [handler http-creds-handler]
  (.mark drpc:num-execute-http-requests)
  (->
    (routes
      (POST "/drpc/:func" [:as {:keys [body servlet-request]} func & m]
        (let [args (slurp body)]
          (populate-context! http-creds-handler servlet-request)
          (.execute handler func args)))
      (POST "/drpc/:func/" [:as {:keys [body servlet-request]} func & m]
        (let [args (slurp body)]
          (populate-context! http-creds-handler servlet-request)
          (.execute handler func args)))
      (GET "/drpc/:func/:args" [:as {:keys [servlet-request]} func args & m]
          (populate-context! http-creds-handler servlet-request)
          (.execute handler func args))
      (GET "/drpc/:func/" [:as {:keys [servlet-request]} func & m]
          (populate-context! http-creds-handler servlet-request)
          (.execute handler func ""))
      (GET "/drpc/:func" [:as {:keys [servlet-request]} func & m]
          (populate-context! http-creds-handler servlet-request)
          (.execute handler func "")))
    (wrap-reload '[org.apache.storm.daemon.drpc])
    handle-request))


(defn launch-server!
  ([]
    (let [conf (clojurify-structure (ConfigUtils/readStormConfig))
          drpc-http-port (int (conf DRPC-HTTP-PORT))
          drpc-server (DrpcServer. conf)
          http-creds-handler (AuthUtils/GetDrpcHttpCredentialsPlugin conf)]
      (when (> drpc-http-port 0)
        (let [app (-> (webapp drpc-server http-creds-handler)
                    requests-middleware)]
          (.setHttpServlet drpc-server (ring.util.servlet/servlet app))))
      (.launchServer drpc-server)))
)

(defn -main []
  (Utils/setupDefaultUncaughtExceptionHandler)
  (launch-server!))
