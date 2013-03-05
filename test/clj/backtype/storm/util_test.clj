(ns backtype.storm.util-test
  (:import [java.util.regex Pattern])
  (:use [clojure test])
  (:use [backtype.storm util]))

(deftest async-loop-test
   (testing "thread name provided"
      (let [thread (async-loop
              (fn []
                (is (= true (.startsWith (.getName (Thread/currentThread)) "Thread-")))
                (is (= true (.endsWith (.getName (Thread/currentThread)) "-mythreadname")))
                1)
              :thread-name "mythreadname")]
      (sleep-secs 2)
      (.interrupt thread)
      (.join thread)))
   (testing "thread name not provided"
      (let [thread (async-loop
              (fn []
                (is (= true (Pattern/matches "Thread-\\d+" (.getName (Thread/currentThread)))))
                1))]
      (sleep-secs 2)
      (.interrupt thread)
      (.join thread))))