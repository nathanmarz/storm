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
(ns backtype.storm.messaging.loader
  (:use [backtype.storm util log])
  (:import [java.util ArrayList])
  (:import [backtype.storm.messaging IContext IConnection TaskMessage])
  (:import [backtype.storm.utils DisruptorQueue MutableObject])
  (:require [backtype.storm.messaging [local :as local]])
  (:require [backtype.storm [disruptor :as disruptor]]))

(defn mk-local-context []
  (local/mk-context))

(defnk launch-receive-thread!
  [context storm-id port transfer-local-fn max-buffer-size
   :daemon true
   :kill-fn (fn [t] (System/exit 1))
   :priority Thread/NORM_PRIORITY]
  (let [max-buffer-size (int max-buffer-size)
        vthread (async-loop
                 (fn []
                   (let [socket (.bind ^IContext context storm-id port)]
                     (fn []
                       (let [batched (ArrayList.)
                             init (.recv ^IConnection socket 0)]
                         (loop [packet init]
                           (let [task (if packet (.task ^TaskMessage packet))
                                 message (if packet (.message ^TaskMessage packet))]
                             (if (= task -1)
                               (do (log-message "Receiving-thread:[" storm-id ", " port "] received shutdown notice")
                                 (.close socket)
                                 nil )
                               (do
                                 (when packet (.add batched [task message]))
                                 (if (and packet (< (.size batched) max-buffer-size))
                                   (recur (.recv ^IConnection socket 1))
                                   (do (transfer-local-fn batched)
                                     0 ))))))))))
                 :factory? true
                 :daemon daemon
                 :kill-fn kill-fn
                 :priority priority)]
    (fn []
      (let [kill-socket (.connect ^IContext context storm-id "localhost" port)]
        (log-message "Shutting down receiving-thread: [" storm-id ", " port "]")
        (.send ^IConnection kill-socket
                  -1
                  (byte-array []))
        (log-message "Waiting for receiving-thread:[" storm-id ", " port "] to die")
        (.join vthread)
        (.close ^IConnection kill-socket)
        (log-message "Shutdown receiving-thread: [" storm-id ", " port "]")
        ))))


