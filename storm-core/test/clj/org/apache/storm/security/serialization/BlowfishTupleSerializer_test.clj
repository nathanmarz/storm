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
(ns org.apache.storm.security.serialization.BlowfishTupleSerializer-test
  (:use [clojure test]
        [org.apache.storm.util :only (exception-cause?)]
        [clojure.string :only (join split)]
  )
  (:import [org.apache.storm.security.serialization BlowfishTupleSerializer]
           [org.apache.storm.utils ListDelegate]
           [com.esotericsoftware.kryo Kryo]
           [com.esotericsoftware.kryo.io Input Output]
  )
)

(deftest test-constructor-throws-on-null-key
  (is (thrown? RuntimeException (new BlowfishTupleSerializer nil {}))
      "Throws RuntimeException when no encryption key is given.")
)

(deftest test-constructor-throws-on-invalid-key
  ; The encryption key must be hexadecimal.
  (let [conf {BlowfishTupleSerializer/SECRET_KEY "0123456789abcdefg"}]
    (is (thrown? RuntimeException (new BlowfishTupleSerializer nil conf))
        "Throws RuntimeException when an invalid encryption key is given.")
  )
)

(deftest test-encrypts-and-decrypts-message
  (let [
        test-text (str
"Tetraodontidae is a family of primarily marine and estuarine fish of the order"
" Tetraodontiformes. The family includes many familiar species, which are"
" variously called pufferfish, puffers, balloonfish, blowfish, bubblefish,"
" globefish, swellfish, toadfish, toadies, honey toads, sugar toads, and sea"
" squab.[1] They are morphologically similar to the closely related"
" porcupinefish, which have large external spines (unlike the thinner, hidden"
" spines of Tetraodontidae, which are only visible when the fish has puffed up)."
" The scientific name refers to the four large teeth, fused into an upper and"
" lower plate, which are used for crushing the shells of crustaceans and"
" mollusks, their natural prey."
)
        kryo (new Kryo)
        arbitrary-key "7dd6fb3203878381b08f9c89d25ed105"
        storm_conf {BlowfishTupleSerializer/SECRET_KEY arbitrary-key}
        writer-bts (new BlowfishTupleSerializer kryo storm_conf)
        reader-bts (new BlowfishTupleSerializer kryo storm_conf)
        buf-size 1024
        output (new Output buf-size buf-size)
        input (new Input buf-size)
        strlist (split test-text #" ")
        delegate (new ListDelegate)
       ]
    (-> delegate (.addAll strlist))
    (-> writer-bts (.write kryo output delegate))
    (.setBuffer input (.getBuffer output))
    (is 
      (=
        test-text
        (join " " (map (fn [e] (str e)) 
          (-> reader-bts (.read kryo input ListDelegate) (.toArray))))
      )
      "Reads a string encrypted by another instance with a shared key"
    )
  )
)
