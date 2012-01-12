(ns backtype.storm.timer
  (:import [backtype.storm.utils Time])
  (:import [java.util PriorityQueue Comparator])
  (:import [java.util.concurrent Semaphore])
  (:use [backtype.storm util])
  (:use [clojure.contrib.def :only [defnk]])
  )

;; The timer defined in this file is very similar to java.util.Timer, except it integrates with
;; Storm's time simulation capabilities. This lets us test code that does asynchronous work on the timer thread

(defnk mk-timer [:kill-fn (fn [& _] )]
  (let [queue (PriorityQueue. 10
                              (reify Comparator
                                (compare [this o1 o2]
                                  (- (first o1) (first o2))
                                  )
                                (equals [this obj]
                                  true
                                  )))
        active (atom true)
        lock (Object.)
        notifier (Semaphore. 0)
        timer-thread (Thread.
                      (fn []
                        (while @active
                          (try
                            (let [[time-secs _ _ :as elem] (.peek queue)]
                              (if elem
                                (if (>= (current-time-secs) time-secs)
                                  ;; can't hold the lock while executing the task function, or else
                                  ;; deadlocks are possible (if the task function locks another lock
                                  ;; and another thread locks that other lock before trying to schedule
                                  ;; something on the timer)
                                  (let [exec-fn (locking lock (second (.poll queue)))]
                                    (exec-fn))
                                  (Time/sleepUntil (to-millis time-secs))
                                  )
                                (Time/sleep 10000)
                                ))

                            (catch InterruptedException e
                              )
                            (catch Throwable t
                              (kill-fn t)
                              (reset! active false)
                              (throw t)
                              )))
                        (.release notifier)))]
    (.setDaemon timer-thread true)
    (.start timer-thread)
    {:timer-thread timer-thread
     :queue queue
     :active active
     :lock lock
     :cancel-notifier notifier}))

(defn- check-active! [timer]
  (when-not @(:active timer)
    (throw (IllegalStateException. "Timer is not active"))))

(defn schedule [timer delay-secs afn]
  (check-active! timer)
  (let [id (uuid)
        ^PriorityQueue queue (:queue timer)]
    (locking (:lock timer)
      (.add queue [(+ (current-time-secs) delay-secs) afn id])
      (when (= id (nth (.peek queue) 2))
        (.interrupt ^Thread (:timer-thread timer)))
      )))

(defn schedule-recurring [timer delay-secs recur-secs afn]
  (check-active! timer)
  (schedule timer
            delay-secs
            (fn this []
              (afn)
              (schedule timer recur-secs this)
              )))

(defn cancel-timer [timer]
  (check-active! timer)
  (locking (:lock timer)
    (reset! (:active timer) false)
    (.interrupt (:timer-thread timer)))
  (.acquire (:cancel-notifier timer)))

(defn timer-waiting? [timer]  
  (Time/isThreadWaiting (:timer-thread timer)))
