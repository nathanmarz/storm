(ns backtype.storm.process-simulator
  (:use [backtype.storm log util])
  )

(def pid-counter (mk-counter))

(def process-map (atom {}))

(def kill-lock (Object.))

(defn register-process [pid shutdownable]
  (swap! process-map assoc pid shutdownable))

(defn process-handle [pid]
  (@process-map pid))

(defn all-processes []
  (vals @process-map))

(defn kill-process [pid]
  (locking kill-lock ; in case cluster shuts down while supervisor is
                     ; killing a task
    (log-message "Killing process " pid)
    (let [shutdownable (process-handle pid)]
      (swap! process-map dissoc pid)
      (when shutdownable
        (.shutdown shutdownable))
      )))

(defn kill-all-processes []
  (doseq [pid (keys @process-map)]
    (kill-process pid)
    ))
