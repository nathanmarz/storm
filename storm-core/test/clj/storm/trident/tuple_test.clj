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
(ns storm.trident.tuple-test
  (:use [clojure test])
  (:require [backtype.storm [testing :as t]])
  (:import [storm.trident.tuple TridentTupleView TridentTupleView$ProjectionFactory
            TridentTupleView$FreshOutputFactory TridentTupleView$OperationOutputFactory
            TridentTupleView$RootFactory])
  (:use [storm.trident testing])
  (:use [backtype.storm util]))

(deftest test-fresh
  (letlocals
    (bind fresh-factory (TridentTupleView$FreshOutputFactory. (fields "a" "b" "c")))
    (bind tt (.create fresh-factory [3 2 1]))
    (is (= [3 2 1] tt))
    (is (= 3 (.getValueByField tt "a")))
    (is (= 2 (.getValueByField tt "b")))
    (is (= 1 (.getValueByField tt "c")))
    ))

(deftest test-projection
  (letlocals
    (bind fresh-factory (TridentTupleView$FreshOutputFactory. (fields "a" "b" "c" "d" "e")))
    (bind project-factory (TridentTupleView$ProjectionFactory. fresh-factory (fields "d" "a")))
    (bind tt (.create fresh-factory [3 2 1 4 5]))
    (bind tt2 (.create fresh-factory [9 8 7 6 10]))
    
    (bind pt (.create project-factory tt))
    (bind pt2 (.create project-factory tt2))
    (is (= [4 3] pt))
    (is (= [6 9] pt2))
    
    (is (= 4 (.getValueByField pt "d")))
    (is (= 3 (.getValueByField pt "a")))
    (is (= 6 (.getValueByField pt2 "d")))
    (is (= 9 (.getValueByField pt2 "a")))
    ))

(deftest test-appends
  (letlocals
    (bind fresh-factory (TridentTupleView$FreshOutputFactory. (fields "a" "b" "c")))
    (bind append-factory (TridentTupleView$OperationOutputFactory. fresh-factory (fields "d" "e")))
    (bind append-factory2 (TridentTupleView$OperationOutputFactory. append-factory (fields "f")))

    (bind tt (.create fresh-factory [1 2 3]))
    (bind tt2 (.create append-factory tt [4 5]))
    (bind tt3 (.create append-factory2 tt2 [7]))
    
    (is (= [1 2 3 4 5 7] tt3))
    (is (= 5 (.getValueByField tt2 "e")))
    (is (= 5 (.getValueByField tt3 "e")))
    (is (= 7 (.getValueByField tt3 "f")))
    ))

(deftest test-root
  (letlocals
    (bind root-factory (TridentTupleView$RootFactory. (fields "a" "b")))
    (bind storm-tuple (t/test-tuple ["a" 1]))
    (bind tt (.create root-factory storm-tuple))
    (is (= ["a" 1] tt))
    (is (= "a" (.getValueByField tt "a")))
    (is (= 1 (.getValueByField tt "b")))
    
    (bind append-factory (TridentTupleView$OperationOutputFactory. root-factory (fields "c")))
    
    (bind tt2 (.create append-factory tt [3]))
    (is (= ["a" 1 3] tt2))
    (is (= "a" (.getValueByField tt2 "a")))
    (is (= 1 (.getValueByField tt2 "b")))
    (is (= 3 (.getValueByField tt2 "c")))
    ))

(deftest test-complex
  (letlocals
    (bind fresh-factory (TridentTupleView$FreshOutputFactory. (fields "a" "b" "c")))
    (bind append-factory1 (TridentTupleView$OperationOutputFactory. fresh-factory (fields "d")))
    (bind append-factory2 (TridentTupleView$OperationOutputFactory. append-factory1 (fields "e" "f")))
    (bind project-factory1 (TridentTupleView$ProjectionFactory. append-factory2 (fields "a" "f" "b")))
    (bind append-factory3 (TridentTupleView$OperationOutputFactory. project-factory1 (fields "c")))
  
    (bind tt (.create fresh-factory [1 2 3]))
    (bind tt2 (.create append-factory1 tt [4]))
    (bind tt3 (.create append-factory2 tt2 [5 6]))
    (bind tt4 (.create project-factory1 tt3))
    (bind tt5 (.create append-factory3 tt4 [8]))
  
    (is (= [1 2 3] tt))
    (is (= [1 2 3 4] tt2))
    (is (= [1 2 3 4 5 6] tt3))
    (is (= [1 6 2] tt4))
    (is (= [1 6 2 8] tt5))
  
    (is (= 1 (.getValueByField tt5 "a")))
    (is (= 6 (.getValueByField tt5 "f")))
    (is (= 2 (.getValueByField tt5 "b")))
    (is (= 8 (.getValueByField tt5 "c")))
    ))

  

