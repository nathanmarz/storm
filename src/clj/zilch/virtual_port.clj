(ns zilch.virtual-port
  (:use [clojure.contrib.def :only [defnk]])
  (:use [backtype.storm util log])
  (:use [backtype.storm.bootstrap])
  (:require [zilch [mq :as mq]])
  (:import [java.nio ByteBuffer])
  (:import [java.util.concurrent Semaphore LinkedBlockingQueue]))

(bootstrap)
(mq/zeromq-imports)

(defn mk-packet [virtual-port ^bytes message]
  (let [bb (ByteBuffer/allocate (+ 2 (count message)))]
    (.putShort bb (short virtual-port))
    (.put bb message)
    (.array bb)
    ))

(defn parse-packet [^bytes packet]
  (let [bb (ByteBuffer/wrap packet)
        port (.getShort bb)
        msg (byte-array (- (count packet) 2))]
    (.get bb msg)
    [port msg]
    ))

(defn virtual-url [port]
  (str "inproc://" port))

(defn virtual-send
  ([^ZMQ$Socket socket virtual-port ^bytes message flags]
     (mq/send socket (mk-packet virtual-port message) flags))
  ([^ZMQ$Socket socket virtual-port ^bytes message]
     (virtual-send socket virtual-port message ZMQ/NOBLOCK)))

(defnk launch-virtual-port!
  [context url receive-queue-map
               :daemon true
               :kill-fn (fn [t] (System/exit 1))
               :priority Thread/NORM_PRIORITY
               :valid-ports nil]
  (let [valid-ports (set (map short valid-ports))
        vthread (async-loop
                  (fn [^ZMQ$Socket socket receive-queue-map]
                        (let [[port msg] (parse-packet (mq/recv socket))]
                          (if (= port -1)
                            (do
                              (log-message "Virtual port " url " received shutdown notice")
                              (.close socket)
                              nil )
                            (if (or (nil? valid-ports) (contains? valid-ports port))
                              (let [port (int port)
                                    ^LinkedBlockingQueue receive-queue (receive-queue-map port)]
                                ;; TODO: probably need to handle multi-part messages here or something
                                (.put receive-queue msg)
                                0
                                )
                              (log-message "Received invalid message directed at port " port ". Dropping...")
                              ))))
                  :args-fn (fn [] [(-> context (mq/socket mq/pull) (mq/bind url)) receive-queue-map])
                  :daemon daemon
                  :kill-fn kill-fn
                  :priority priority)]
    (fn []
      (let [kill-socket (-> context (mq/socket mq/push) (mq/connect url))]
        (log-message "Shutting down virtual port at url: " url)
        (virtual-send kill-socket
                      -1
                      (mq/barr 1))
        (.close kill-socket)
        (log-message "Waiting for virtual port at url " url " to die")
        (.join vthread)
        (log-message "Shutdown virtual port at url: " url)
        ))))

(defn launch-fake-virtual-port! [context storm-id port receive-queue-map deserializer]
  (let [socket (-> context (msg/bind storm-id port))
        vthread (async-loop
                 (fn []
                   (let [[port ser-msg] (msg/recv socket)
                         msg (if (nil? ser-msg)
                               nil
                               (.deserialize deserializer ser-msg))
                         ]
                     (if (= (int port) -1)
                       (do
                         (log-message "FAKE virtual port received shutdown notice")
                         (.close socket)
                         nil )
                       (let [port (int port)
                             receive-queue (receive-queue-map port)]
                         (.put receive-queue msg)
                         0
                         ))))
                 )]
    (fn []
      (let [kill-socket (-> context (msg/connect storm-id nil port))]
        (log-message "Shutting down FAKE virtual port")
        (msg/send kill-socket -1 nil)
        (.close kill-socket)
        (log-message "Waiting for FAKE virtual port to die")
        (.join vthread)
        (log-message "Shutdown FAKE virtual port")
                                    ))))
(defn virtual-bind
  [^ZMQ$Socket socket virtual-port]
  (mq/bind socket (virtual-url virtual-port))
  )

(defn virtual-connect
  [^ZMQ$Socket socket virtual-port]
  (mq/connect socket (virtual-url virtual-port))
  )
