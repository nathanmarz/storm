(ns backtype.storm.config
  (:import [org.jvyaml YAML])
  (:import [java.io FileReader File])
  (:import [backtype.storm Config])
  (:import [backtype.storm.utils Utils LocalState])
  (:import [org.apache.commons.io FileUtils])
  (:require [clojure.string :as str])
  (:use [backtype.storm util])
  )

(def RESOURCES-SUBDIR "resources")

;; define clojure constants for every configuration parameter
(doseq [f (seq (.getFields Config))]
  (let [name (.getName f)
        new-name (.replace (.toUpperCase name) "_" "-")]
    (eval
      `(def ~(symbol new-name) (. Config ~(symbol name))))
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

(defn mk-zk-connect-string [conf]
  (let [servers (conf STORM-ZOOKEEPER-SERVERS)
        port (conf STORM-ZOOKEEPER-PORT)
        root (conf STORM-ZOOKEEPER-ROOT)]
    (str
      (str/join ","
        (for [s servers]
          (str s ":" port)))
      root)
    ))

(defn read-default-config []
  (clojurify-structure (Utils/readDefaultConfig)))

(defn read-storm-config []
  (clojurify-structure (Utils/readStormConfig)))

(defn read-yaml-config [name]
  (clojurify-structure (Utils/findAndReadConfigFile name true)))

(defn master-stormdist-root [conf storm-id]
  (str (conf STORM-LOCAL-DIR) "/stormdist/" storm-id))

(defn master-stormjar-path [stormroot]
  (str stormroot "/stormjar.jar"))

(defn master-stormcode-path [stormroot]
  (str stormroot "/stormcode.ser"))

(defn master-stormconf-path [stormroot]
  (str stormroot "/stormconf.ser"))

(defn master-inbox [conf]
  (let [ret (str (conf STORM-LOCAL-DIR) "/inbox")]
    (FileUtils/forceMkdir (File. ret))
    ret ))

(defn supervisor-stormdist-root
  ([conf] (str (conf STORM-LOCAL-DIR) "/stormdist"))
  ([conf storm-id]
      (str (supervisor-stormdist-root conf) "/" storm-id)))

(defn supervisor-stormjar-path [stormroot]
  (str stormroot "/stormjar.jar"))

(defn supervisor-stormcode-path [stormroot]
  (str stormroot "/stormcode.ser"))

(defn supervisor-stormconf-path [stormroot]
  (str stormroot "/stormconf.ser"))

(defn supervisor-tmp-dir [conf]
  (let [ret (str (conf STORM-LOCAL-DIR) "/tmp")]
    (FileUtils/forceMkdir (File. ret))
    ret ))

(defn supervisor-storm-resources-path [stormroot]
  (str stormroot "/" RESOURCES-SUBDIR))

(defn ^LocalState supervisor-state [conf]
  (LocalState. (str (conf STORM-LOCAL-DIR) "/localstate")))


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
