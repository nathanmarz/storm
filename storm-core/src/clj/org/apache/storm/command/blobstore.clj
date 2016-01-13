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
(ns org.apache.storm.command.blobstore
  (:import [java.io InputStream OutputStream]
           [org.apache.storm.generated SettableBlobMeta AccessControl AuthorizationException
            KeyNotFoundException]
           [org.apache.storm.blobstore BlobStoreAclHandler])
  (:use [org.apache.storm config]
        [clojure.string :only [split]]
        [clojure.tools.cli :only [cli]]
        [clojure.java.io :only [copy input-stream output-stream]]
        [org.apache.storm blobstore log util])
  (:gen-class))

(defn update-blob-from-stream
  "Update a blob in the blob store from an InputStream"
  [key ^InputStream in]
  (with-configured-blob-client blobstore
    (let [out (.updateBlob blobstore key)]
      (try 
        (copy in out)
        (.close out)
        (catch Exception e
          (log-message e)
          (.cancel out)
          (throw e))))))

(defn create-blob-from-stream
  "Create a blob in the blob store from an InputStream"
  [key ^InputStream in ^SettableBlobMeta meta]
  (with-configured-blob-client blobstore
    (let [out (.createBlob blobstore key meta)]
      (try 
        (copy in out)
        (.close out)
        (catch Exception e
          (.cancel out)
          (throw e))))))

(defn read-blob
  "Read a blob in the blob store and write to an OutputStream"
  [key ^OutputStream out]
  (with-configured-blob-client blobstore
    (with-open [in (.getBlob blobstore key)]
      (copy in out))))

(defn as-access-control
  "Convert a parameter to an AccessControl object"
  [param]
  (BlobStoreAclHandler/parseAccessControl (str param)))

(defn as-acl
  [param]
  (map as-access-control (split param #",")))

(defn access-control-str
  [^AccessControl acl]
  (BlobStoreAclHandler/accessControlToString acl))

(defn read-cli [args]
  (let [[{file :file} [key] _] (cli args ["-f" "--file" :default nil])]
    (if file
      (with-open [f (output-stream file)]
        (read-blob key f))
      (read-blob key System/out))))

(defn update-cli [args]
  (let [[{file :file} [key] _] (cli args ["-f" "--file" :default nil])]
    (if file
      (with-open [f (input-stream file)]
        (update-blob-from-stream key f))
      (update-blob-from-stream key System/in))
    (log-message "Successfully updated " key)))

(defn create-cli [args]
  (let [[{file :file acl :acl replication-factor :replication-factor} [key] _] (cli args ["-f" "--file" :default nil]
                                                  ["-a" "--acl" :default [] :parse-fn as-acl]
                                                  ["-r" "--replication-factor" :default -1 :parse-fn parse-int])
        meta (doto (SettableBlobMeta. acl)
                   (.set_replication_factor replication-factor))]
    (validate-key-name! key)
    (log-message "Creating " key " with ACL " (pr-str (map access-control-str acl)))
    (if file
      (with-open [f (input-stream file)]
        (create-blob-from-stream key f meta))
      (create-blob-from-stream key System/in meta))
    (log-message "Successfully created " key)))

(defn delete-cli [args]
  (with-configured-blob-client blobstore
    (doseq [key args]
      (.deleteBlob blobstore key)
      (log-message "deleted " key))))

(defn list-cli [args]
  (with-configured-blob-client blobstore
    (let [keys (if (empty? args) (iterator-seq (.listKeys blobstore)) args)]
      (doseq [key keys]
        (try
          (let [meta (.getBlobMeta blobstore key)
                version (.get_version meta)
                acl (.get_acl (.get_settable meta))]
            (log-message key " " version " " (pr-str (map access-control-str acl))))
          (catch AuthorizationException ae
            (if-not (empty? args) (log-error "ACCESS DENIED to key: " key)))
          (catch KeyNotFoundException knf
            (if-not (empty? args) (log-error key " NOT FOUND"))))))))

(defn set-acl-cli [args]
  (let [[{set-acl :set} [key] _]
           (cli args ["-s" "--set" :default [] :parse-fn as-acl])]
    (with-configured-blob-client blobstore
      (let [meta (.getBlobMeta blobstore key)
            acl (.get_acl (.get_settable meta))
            new-acl (if set-acl set-acl acl)
            new-meta (SettableBlobMeta. new-acl)]
        (log-message "Setting ACL for " key " to " (pr-str (map access-control-str new-acl)))
        (.setBlobMeta blobstore key new-meta)))))

(defn rep-cli [args]
  (let [sub-command (first args)
        new-args (rest args)]
    (with-configured-blob-client blobstore
      (condp = sub-command
      "--read" (let [key (first new-args)
                     blob-replication (.getBlobReplication blobstore key)]
                 (log-message "Current replication factor " blob-replication)
                 blob-replication)
      "--update" (let [[{replication-factor :replication-factor} [key] _]
                        (cli new-args ["-r" "--replication-factor" :parse-fn parse-int])]
                   (if (nil? replication-factor)
                     (throw (RuntimeException. (str "Please set the replication factor")))
                     (let [blob-replication (.updateBlobReplication blobstore key replication-factor)]
                       (log-message "Replication factor is set to " blob-replication)
                       blob-replication)))
      :else (throw (RuntimeException. (str sub-command " is not a supported blobstore command")))))))

(defn -main [& args]
  (let [command (first args)
        new-args (rest args)]
    (condp = command
      "cat" (read-cli new-args)
      "create" (create-cli new-args)
      "update" (update-cli new-args)
      "delete" (delete-cli new-args)
      "list" (list-cli new-args)
      "set-acl" (set-acl-cli new-args)
      "replication" (rep-cli new-args)
      :else (throw (RuntimeException. (str command " is not a supported blobstore command"))))))
