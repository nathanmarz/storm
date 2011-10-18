(ns zilch.test.mq
  (:use clojure.test)
  (:import [java.util Arrays UUID])
  (:require [zilch.mq :as mq]))

(defn uuid [] (str (UUID/randomUUID)))

(defn random-msg []
  (byte-array (map byte (for [i (range (Integer. (int (rand 100))))]
    (Integer. (int (rand 100)))
    ))))

(def url
     (str "inproc://" (uuid))
     ;; (str "ipc://" (uuid))
     ;; (str "tcp://127.0.0.1:" (+ 4000 (Math/round (rand 1000)))))
     )

(deftest zilch
  (testing "zilch"
    (testing "should be able to"

      (testing "push / pull"
        (mq/with-context context 2
          (with-open [s0 (-> context
                             (mq/socket mq/pull)
                             (mq/bind url))
                      s1 (-> context
                             (mq/socket mq/push)
                             (mq/connect url))]
            (let [msg (random-msg)
                  push (future (mq/send s1 msg))
                  pull (future (mq/recv s0))]
              (is (Arrays/equals msg @pull))))))

      (testing "pub / sub"
        (mq/with-context context 2
          (with-open [s0 (-> context
                             (mq/socket mq/pub)
                             (mq/bind url))
                      s1 (-> context
                             (mq/socket mq/sub)
                             (mq/subscribe)
                             (mq/connect url))]
            (let [msg (random-msg)
                  pub (future (mq/send s0 msg))
                  sub (future (mq/recv s1))]
              (is (Arrays/equals msg @sub))))))

      (testing "pair / pair"
        (mq/with-context context 2
          (with-open [s0 (-> context
                             (mq/socket mq/pair)
                             (mq/bind url))
                      s1 (-> context
                             (mq/socket mq/pair)
                             (mq/connect url))]
            (let [msg0 (random-msg)
                  pair0 (future (mq/send s0 msg0)
                                (mq/recv s0))
                  msg1 (random-msg)
                  pair1 (future (mq/send s1 msg1)
                                (mq/recv s1))]
              (is (Arrays/equals msg1 @pair0))
              (is (Arrays/equals msg0 @pair1))))))

      (testing "req / rep"
        (mq/with-context context 2
          (with-open [s0 (-> context
                             (mq/socket mq/rep)
                             (mq/bind url))
                      s1 (-> context
                             (mq/socket mq/req)
                             (mq/connect url))]
            (let [msg (random-msg)
                  req (future (mq/send s1 msg)
                              (mq/recv s1))
                  rep (future (mq/recv s0)
                              (mq/send s0 msg))]
              (is (Arrays/equals msg @req))))))

      (testing "req / xrep")

      (testing "xreq / rep")

      (testing "xreq / xrep"))))
