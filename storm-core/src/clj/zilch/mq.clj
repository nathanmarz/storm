;; Copyright 2011 Tim Dysinger

;;    Licensed under the Apache License, Version 2.0 (the "License");
;;    you may not use this file except in compliance with the License.
;;    You may obtain a copy of the License at

;;        http://www.apache.org/licenses/LICENSE-2.0

;;    Unless required by applicable law or agreed to in writing, software
;;    distributed under the License is distributed on an "AS IS" BASIS,
;;    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;;    See the License for the specific language governing permissions and
;;    limitations under the License.

(ns zilch.mq
  (:refer-clojure :exclude [send])
  )

(defmacro zeromq-imports []
  '(do
    (import '[org.zeromq ZMQ ZMQ$Context ZMQ$Socket])
    ))

(zeromq-imports)

(defn ^ZMQ$Context context [threads]
  (ZMQ/context threads))

(defmacro with-context
  [id threads & body]
  `(let [~id (context ~threads)]
     (try ~@body
          (finally (.term ~id)))))

(def sndmore ZMQ/SNDMORE)

(def req ZMQ/REQ)
(def rep ZMQ/REP)
(def xreq ZMQ/XREQ)
(def xrep ZMQ/XREP)
(def pub ZMQ/PUB)
(def sub ZMQ/SUB)
(def pair ZMQ/PAIR)
(def push ZMQ/PUSH)
(def pull ZMQ/PULL)

(defn ^bytes barr [& arr]
  (byte-array (map byte arr)))

(defn ^ZMQ$Socket socket
  [^ZMQ$Context context type]
  (.socket context type))

(defn set-linger
  [^ZMQ$Socket socket linger-ms]
  (doto socket
    (.setLinger (long linger-ms))))

(defn set-hwm
  [^ZMQ$Socket socket hwm]
  (if hwm
    (doto socket
      (.setHWM (long hwm)))
    socket
    ))

(defn bind
  [^ZMQ$Socket socket url]
  (doto socket
    (.bind url)))

(defn connect
  [^ZMQ$Socket socket url]
  (doto socket
    (.connect url)))

(defn subscribe
  ([^ZMQ$Socket socket ^bytes topic]
     (doto socket
       (.subscribe topic)))
  ([^ZMQ$Socket socket]
     (subscribe socket (byte-array []))))

(defn unsubscribe
  ([^ZMQ$Socket socket ^bytes topic]
     (doto socket
       (.unsubscribe (.getBytes topic))))
  ([^ZMQ$Socket socket]
     (unsubscribe socket "")))

(defn send
  ([^ZMQ$Socket socket ^bytes message flags]
     (.send socket message flags))
  ([^ZMQ$Socket socket ^bytes message]
     (send socket message ZMQ/NOBLOCK)))

(defn recv-more? [^ZMQ$Socket socket]
  (.hasReceiveMore socket))

(defn recv
  ([^ZMQ$Socket socket flags]
     (.recv socket flags))
  ([^ZMQ$Socket socket]
     (recv socket 0)))
