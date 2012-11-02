(ns backtype.storm.metric.testing
  "This namespace is for AOT dependent metrics testing code."
  (:gen-class))

(gen-class
 :name clojure.storm.metric.testing.FakeMetricConsumer
 :implements [backtype.storm.metric.api.IMetricsConsumer]
 :prefix "impl-"
 :state state
 :init init)

(defn impl-init [] [[] (atom [])])

(defn impl-prepare [this conf {:keys [ns var-name]} ctx error-reporter]
  (reset! (.state this) @(intern ns var-name))
  (reset! @(.state this) []))

(defn impl-cleanup [this]
  (reset! @(.state this) []))

(defn impl-handleDataPoints [this task-info data-points]
  (swap! @(.state this) conj [task-info data-points]))
 

