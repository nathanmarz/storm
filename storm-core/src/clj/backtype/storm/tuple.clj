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
(ns backtype.storm.tuple
  (:use [backtype.storm bootstrap])
  )

(bootstrap)

(def ARRAY-TYPES
  #{
    (type (to-array []))
    (type (byte-array 0))
    (type (char-array 0))
    (type (short-array 0))
    (type (int-array 0))
    (type (long-array 0))
    (type (float-array 0))
    (type (double-array 0))
    (type (boolean-array 0))
  })

(declare hash-code)

(defn deep-hash-code [alist]
  "deep hash code based on array/list content.
   it's the same as java.util.Arrays.deepHashCode(),
   but without convert a list to array to avoid copy.

   use unchecked-* to make sure use int instead of long."
  (reduce
    #(unchecked-add-int (unchecked-multiply-int 31 %1) (hash-code %2))
    1 alist))

(defn hash-code [obj]
  (let [t (type obj)]
    (if (contains? ARRAY-TYPES t)
      (deep-hash-code obj)
      (.hashCode obj))))

(defn list-hash-code [^List alist]
  (deep-hash-code alist))

