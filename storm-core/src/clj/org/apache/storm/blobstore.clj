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

(ns org.apache.storm.blobstore
  (:import [org.apache.storm.utils Utils ConfigUtils])
  (:import [org.apache.storm.blobstore ClientBlobStore])
  (:use [org.apache.storm config util]))

(defmacro with-configured-blob-client
  [client-sym & body]
  `(let [conf# (clojurify-structure (ConfigUtils/readStormConfig))
         ^ClientBlobStore ~client-sym (Utils/getClientBlobStore conf#)]
     (try
       ~@body
       (finally (.shutdown ~client-sym)))))
