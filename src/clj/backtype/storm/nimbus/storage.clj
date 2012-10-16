(ns backtype.storm.nimbus.storage
  (:import [java.io InputStream OutputStream FileInputStream FileOutputStream File])
  (:import [java.util List Map])
  (:import [org.apache.commons.io FileUtils IOUtils])
  (:use [backtype.storm log config util])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm.nimbus INimbusStorage]))

(defn create-local-storage [conf]
  (let [stormroot (nimbus-storage-local-dir conf)]
    (log-message "Using default storage (" stormroot ")")
    (reify INimbusStorage
      (^InputStream open [this, ^String path]
        (FileInputStream. (str stormroot "/" path)))

      (^OutputStream create [this, ^String path]
        (FileOutputStream. (str stormroot "/" path)))

      (^List list [this, ^String path]
        (if (exists-file? path)
          (.list (File. path))
          []))

      (^void delete [this, ^String path]
        (when (exists-file? path)
          (FileUtils/forceDelete (File. path))))

      (^void delete [this, ^List paths]
        (doseq [path paths]
          (.delete this path)))

      (^void mkdirs [this, ^String path]
        (FileUtils/forceMkdir (File. path)))

      (^void move [this, ^String from, ^String to]
        (FileUtils/moveFile (File. from) (File. to)))

      (^boolean isSupportDistributed [this]
        false))))

(defn create-custom-storage [storage-name conf]
  (let [storage (new-instance storage-name)]
    (.init storage conf)
    (log-message "Using custom storage: " storage-name)
    storage))

(defn ^INimbusStorage create-nimbus-storage [conf]
  (if-let [storage-name (conf NIMBUS-STORAGE)]
    (create-custom-storage storage-name conf)
    (create-local-storage conf)))

(defn upload-file-to-storage [file storage path]
  (let [stream (.create storage path)]
    (try
      (IOUtils/copy (FileInputStream. file) stream)
      (finally (.close stream)))))

(defn ensure-clean-dir-in-storage [storage path]
  (.mkdirs storage path)
  (if-let [files (.list storage path)]
    (.delete storage files)))

(defn serialize-to-storage [obj storage path]
  (let [stream (.create storage path)]
    (try
      (IOUtils/write (Utils/serialize obj) stream)
      (finally (.close stream)))))
