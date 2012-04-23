(ns backtype.storm.config
  (:import [java.io FileReader File])
  (:import [backtype.storm Config])
  (:import [backtype.storm.utils Utils LocalState])
  (:import [org.apache.commons.io FileUtils])
  (:require [clojure [string :as str]])
  (:use [backtype.storm util])
  )

(def RESOURCES-SUBDIR "resources")

(defn- clojure-config-name [name]
  (.replace (.toUpperCase name) "_" "-"))

;; define clojure constants for every configuration parameter
(doseq [f (seq (.getFields Config))]
  (let [name (.getName f)
        new-name (clojure-config-name name)]
    (eval
      `(def ~(symbol new-name) (. Config ~(symbol name))))
      ))

(def ALL-CONFIGS
  (dofor [f (seq (.getFields Config))]
         (.get f nil)
         ))

(defn cluster-mode [conf & args]
  (keyword (conf STORM-CLUSTER-MODE)))

(defn local-mode? [conf]
  (let [mode (conf STORM-CLUSTER-MODE)]
    (condp = mode
      "local" true
      "distributed" false
      (throw (IllegalArgumentException.
                (str "Illegal cluster mode in conf: " mode)))
      )))

(defn sampling-rate [conf]
  (->> (conf TOPOLOGY-STATS-SAMPLE-RATE)
       (/ 1)
       int))

(defn mk-stats-sampler [conf]
  (even-sampler (sampling-rate conf)))

; storm.zookeeper.servers:
;     - "server1"
;     - "server2"
;     - "server3"
; nimbus.host: "master"
; 
; ########### These all have default values as shown
; 
; ### storm.* configs are general configurations
; # the local dir is where jars are kept
; storm.local.dir: "/mnt/storm"
; storm.zookeeper.port: 2181
; storm.zookeeper.root: "/storm"

(defn read-default-config []
  (clojurify-structure (Utils/readDefaultConfig)))

(defn read-storm-config []
  (clojurify-structure (Utils/readStormConfig)))

(defn read-yaml-config [name]
  (clojurify-structure (Utils/findAndReadConfigFile name true)))

(defn master-local-dir [conf]
  (let [ret (str (conf STORM-LOCAL-DIR) "/nimbus")]
    (FileUtils/forceMkdir (File. ret))
    ret
    ))

(defn master-stormdist-root
  ([conf]
     (str (master-local-dir conf) "/stormdist"))
  ([conf storm-id]
     (str (master-stormdist-root conf) "/" storm-id)))

(defn master-stormjar-path [stormroot]
  (str stormroot "/stormjar.jar"))

(defn master-stormcode-path [stormroot]
  (str stormroot "/stormcode.ser"))

(defn master-stormconf-path [stormroot]
  (str stormroot "/stormconf.ser"))

(defn master-inbox [conf]
  (let [ret (str (master-local-dir conf) "/inbox")]
    (FileUtils/forceMkdir (File. ret))
    ret ))

(defn master-inimbus-dir [conf]
  (str (master-local-dir conf) "/inimbus"))

(defn supervisor-local-dir [conf]
  (let [ret (str (conf STORM-LOCAL-DIR) "/supervisor")]
    (FileUtils/forceMkdir (File. ret))
    ret
    ))

(defn supervisor-isupervisor-dir [conf]
  (str (supervisor-local-dir conf) "/isupervisor"))

(defn supervisor-stormdist-root
  ([conf] (str (supervisor-local-dir conf) "/stormdist"))
  ([conf storm-id]
      (str (supervisor-stormdist-root conf) "/" (java.net.URLEncoder/encode storm-id))))

(defn supervisor-stormjar-path [stormroot]
  (str stormroot "/stormjar.jar"))

(defn supervisor-stormcode-path [stormroot]
  (str stormroot "/stormcode.ser"))

(defn supervisor-stormconf-path [stormroot]
  (str stormroot "/stormconf.ser"))

(defn supervisor-tmp-dir [conf]
  (let [ret (str (supervisor-local-dir conf) "/tmp")]
    (FileUtils/forceMkdir (File. ret))
    ret ))

(defn supervisor-storm-resources-path [stormroot]
  (str stormroot "/" RESOURCES-SUBDIR))

(defn ^LocalState supervisor-state [conf]
  (LocalState. (str (supervisor-local-dir conf) "/localstate")))

(defn read-supervisor-storm-conf [conf storm-id]
  (let [stormroot (supervisor-stormdist-root conf storm-id)
        conf-path (supervisor-stormconf-path stormroot)
        topology-path (supervisor-stormcode-path stormroot)]
    (merge conf (Utils/deserialize (FileUtils/readFileToByteArray (File. conf-path))))
    ))

(defn read-supervisor-topology [conf storm-id]
  (let [stormroot (supervisor-stormdist-root conf storm-id)
        topology-path (supervisor-stormcode-path stormroot)]
    (Utils/deserialize (FileUtils/readFileToByteArray (File. topology-path)))
    ))

(defn worker-root
  ([conf]
     (str (conf STORM-LOCAL-DIR) "/workers"))
  ([conf id]
     (str (worker-root conf) "/" id)))

(defn worker-pids-root
  [conf id]
  (str (worker-root conf id) "/pids"))

(defn worker-pid-path [conf id pid]
  (str (worker-pids-root conf id) "/" pid))

(defn worker-heartbeats-root
  [conf id]
  (str (worker-root conf id) "/heartbeats"))

;; workers heartbeat here with pid and timestamp
;; if supervisor stops receiving heartbeat, it kills and restarts the process
;; in local mode, keep a global map of ids to threads for simulating process management
(defn ^LocalState worker-state  [conf id]
  (LocalState. (worker-heartbeats-root conf id)))
