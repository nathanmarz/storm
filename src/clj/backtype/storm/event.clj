(ns backtype.storm.event
  (:use [backtype.storm log util])
  (:import [backtype.storm.utils Time Utils])
  (:import [java.util.concurrent LinkedBlockingQueue TimeUnit])
  )

(defprotocol EventManager
  (add [this event-fn])
  (waiting? [this])
  (shutdown [this]))

(defn event-manager
  "Creates a thread to respond to events. Any error will cause process to halt"
  [daemon?]
  (let [added (atom 0)
        processed (atom 0)
        ^LinkedBlockingQueue queue (LinkedBlockingQueue.)
        running (atom true)
        runner (Thread.
                  (fn []
                    (try-cause
                      (while @running
                        (let [r (.take queue)]
                          (r)
                          (swap! processed inc)))
                    (catch InterruptedException t
                      (log-message "Event manager interrupted"))
                    (catch Throwable t
                      (log-error t "Error when processing event")
                      (halt-process! 20 "Error when processing an event"))
                      )))]
    (.setDaemon runner daemon?)
    (.start runner)
    (reify
      EventManager
      (add [this event-fn]
        ;; should keep track of total added and processed to know if this is finished yet
        (when-not @running
          (throw (RuntimeException. "Cannot add events to a shutdown event manager")))
        (swap! added inc)
        (.put queue event-fn)
        )
      (waiting? [this]
        (or (Time/isThreadWaiting runner)
            (= @processed @added)
            ))
      (shutdown [this]
        (reset! running false)
        (.interrupt runner)
        (.join runner)
        )
        )))
