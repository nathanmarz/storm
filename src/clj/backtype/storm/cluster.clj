(ns backtype.storm.cluster
  (:import [org.apache.zookeeper.data Stat])
  (:import [backtype.storm.utils Utils])
  (:use [backtype.storm util log config])
  (:use [clojure.contrib.core :only [dissoc-in]])
  (:require [backtype.storm [zookeeper :as zk]])
  )

(defprotocol ClusterState
  (set-ephemeral-node [this path data])
  (delete-node [this path])
  (set-data [this path data])  ;; if node does not exist, create persistent with this data 
  (get-data [this path watch?])
  (get-children [this path watch?])
  (mkdirs [this path])
  (close [this])
  (register [this callback])
  (unregister [this id])
  )

(defn mk-distributed-cluster-state [conf]
  (let [zk (zk/mk-client (mk-zk-connect-string (assoc conf STORM-ZOOKEEPER-ROOT "/")))]
    (zk/mkdirs zk (conf STORM-ZOOKEEPER-ROOT))
    (.close zk)
    )
  (let [callbacks (atom {})
        active (atom true)
        mk-zk #(zk/mk-client (mk-zk-connect-string conf)
                             (conf STORM-ZOOKEEPER-SESSION-TIMEOUT)
                             %)
        zk (atom nil)
        watcher (fn this [state type path]
                    (when @active
                      (when-not (= :connected state)
                        (log-message "Zookeeper disconnected. Attempting to reconnect")
                        (reset! zk (mk-zk this))
                        )
                      (when-not (= :none type)
                        (doseq [callback (vals @callbacks)]                          
                          (callback type path))))
                      )]
    (reset! zk (mk-zk watcher))
    (reify
     ClusterState
     (register [this callback]
               (let [id (uuid)]
                 (swap! callbacks assoc id callback)
                 id
                 ))
     (unregister [this id]
                 (swap! callbacks dissoc id))
     (set-ephemeral-node [this path data]
                         (zk/mkdirs @zk (parent-path path))
                         (if (zk/exists @zk path false)
                           (zk/set-data @zk path data) ; should verify that it's ephemeral
                           (zk/create-node @zk path data :ephemeral)
                           ))
     
     (set-data [this path data]
               ;; note: this does not turn off any existing watches
               (if (zk/exists @zk path false)
                 (zk/set-data @zk path data)
                 (do
                   (zk/mkdirs @zk (parent-path path))
                   (zk/create-node @zk path data :persistent)
                   )))
     
     (delete-node [this path]
                  (zk/delete-recursive @zk path)
                  )
     
     (get-data [this path watch?]
               (zk/get-data @zk path watch?)
               )
     
     (get-children [this path watch?]
                   (zk/get-children @zk path watch?))
     
     (mkdirs [this path]
             (zk/mkdirs @zk path))
     
     (close [this]
            (reset! active false)
            (.close @zk))
     )))

(defprotocol StormClusterState
  (assignments [this callback])
  (assignment-info [this storm-id callback])
  (active-storms [this])
  (storm-base [this storm-id callback])

  (task-storms [this])
  (task-ids [this storm-id])
  (task-info [this storm-id task-id])
  (task-heartbeat [this storm-id task-id]) ;; returns nil if doesn't exist
  (supervisors [this callback])
  (supervisor-info [this supervisor-id])  ;; returns nil if doesn't exist

  (setup-heartbeats! [this storm-id])
  (teardown-heartbeats! [this storm-id])
  (teardown-task-errors! [this storm-id])
  (heartbeat-storms [this])
  (task-error-storms [this])
  (heartbeat-tasks [this storm-id])

  (set-task! [this storm-id task-id info])
  (task-heartbeat! [this storm-id task-id info])
  (remove-task-heartbeat! [this storm-id task-id])
  (supervisor-heartbeat! [this supervisor-id info])
  (activate-storm! [this storm-id storm-base])
  (deactivate-storm! [this storm-id])
  (set-assignment! [this storm-id info])
  (remove-storm! [this storm-id])
  (report-task-error [this storm-id task-id error])
  (task-errors [this storm-id task-id])

  (disconnect [this])
  )


(def ASSIGNMENTS-ROOT "assignments")
(def TASKS-ROOT "tasks")
(def CODE-ROOT "code")
(def STORMS-ROOT "storms")
(def SUPERVISORS-ROOT "supervisors")
(def TASKBEATS-ROOT "taskbeats")
(def TASKERRORS-ROOT "taskerrors")

(def ASSIGNMENTS-SUBTREE (str "/" ASSIGNMENTS-ROOT))
(def TASKS-SUBTREE (str "/" TASKS-ROOT))
(def STORMS-SUBTREE (str "/" STORMS-ROOT))
(def SUPERVISORS-SUBTREE (str "/" SUPERVISORS-ROOT))
(def TASKBEATS-SUBTREE (str "/" TASKBEATS-ROOT))
(def TASKERRORS-SUBTREE (str "/" TASKERRORS-ROOT))

(defn supervisor-path [id]
  (str SUPERVISORS-SUBTREE "/" id))

(defn assignment-path [id]
  (str ASSIGNMENTS-SUBTREE "/" id))

(defn storm-path [id]
  (str STORMS-SUBTREE "/" id))

(defn storm-task-root [storm-id]
  (str TASKS-SUBTREE "/" storm-id))

(defn task-path [storm-id task-id]
  (str (storm-task-root storm-id) "/" task-id))

(defn taskbeat-storm-root [storm-id]
  (str TASKBEATS-SUBTREE "/" storm-id))

(defn taskbeat-path [storm-id task-id]
  (str (taskbeat-storm-root storm-id) "/" task-id))

(defn taskerror-storm-root [storm-id]
  (str TASKERRORS-SUBTREE "/" storm-id))

(defn taskerror-path [storm-id task-id]
  (str (taskerror-storm-root storm-id) "/" task-id))


(defn- issue-callback! [cb-atom]
  (let [cb @cb-atom]
    (reset! cb-atom nil)
    (when cb
      (cb))
    ))

(defn- issue-map-callback! [cb-atom id]
  (let [cb (@cb-atom id)]
    (swap! cb-atom dissoc id)
    (when cb
      (cb id))
    ))

(defn- maybe-deserialize [ser]
  (when ser
    (Utils/deserialize ser)))

(defstruct TaskError :error :time-secs)

(defn mk-storm-cluster-state [cluster-state-spec]
  (let [[solo? cluster-state] (if (satisfies? ClusterState cluster-state-spec)
                                [false cluster-state-spec]
                                [true (mk-distributed-cluster-state cluster-state-spec)])
        assignment-info-callback (atom {})
        supervisors-callback (atom nil)
        assignments-callback (atom nil)
        storm-base-callback (atom {})
        state-id (register
                  cluster-state
                  (fn [type path]
                    (let [[subtree & args] (tokenize-path path)]
                      (condp = subtree
                          ASSIGNMENTS-ROOT (if (empty? args)
                                             (issue-callback! assignments-callback)
                                             (issue-map-callback! assignment-info-callback (first args)))
                          SUPERVISORS-ROOT (issue-callback! supervisors-callback)
                          STORMS-ROOT (issue-map-callback! storm-base-callback (first args))
                          ;; this should never happen
                          (halt-process! 30 "Unknown callback for subtree " subtree args)
                          )
                      )))]
    (doseq [p [ASSIGNMENTS-SUBTREE TASKS-SUBTREE STORMS-SUBTREE SUPERVISORS-SUBTREE TASKBEATS-SUBTREE TASKERRORS-SUBTREE]]
      (mkdirs cluster-state p))
    (reify
     StormClusterState
     
     (assignments [this callback]
        (when callback
          (reset! assignments-callback callback))
        (get-children cluster-state ASSIGNMENTS-SUBTREE (not-nil? callback)))
      
      (assignment-info [this storm-id callback]
        (when callback
          (swap! assignment-info-callback assoc storm-id callback))
        (maybe-deserialize (get-data cluster-state (assignment-path storm-id) (not-nil? callback)))
        )

      (active-storms [this]
        (get-children cluster-state STORMS-SUBTREE false)
        )

      (heartbeat-storms [this]
        (get-children cluster-state TASKBEATS-SUBTREE false)
        )

      (task-error-storms [this]
         (get-children cluster-state TASKERRORS-SUBTREE false)
         )
      
      (heartbeat-tasks [this storm-id]
        (get-children cluster-state (taskbeat-storm-root storm-id) false)
        )

      (task-storms [this]
        (get-children cluster-state TASKS-SUBTREE false)
        )

      (task-ids [this storm-id]
        (map parse-int
          (get-children cluster-state (storm-task-root storm-id) false)
          ))

      (task-info [this storm-id task-id]
        (maybe-deserialize (get-data cluster-state (task-path storm-id task-id) false))
        )

      ;; TODO: add a callback here so that nimbus can respond immediately when it goes down? 
      (task-heartbeat [this storm-id task-id]
        (maybe-deserialize (get-data cluster-state (taskbeat-path storm-id task-id) false))
        )

      (supervisors [this callback]
        (when callback
          (reset! supervisors-callback callback))
        (get-children cluster-state SUPERVISORS-SUBTREE (not-nil? callback))
        )

      (supervisor-info [this supervisor-id]
        (maybe-deserialize (get-data cluster-state (supervisor-path supervisor-id) false))
        )

      (set-task! [this storm-id task-id info]
        (set-data cluster-state (task-path storm-id task-id) (Utils/serialize info))
        )

      (task-heartbeat! [this storm-id task-id info]
        (set-ephemeral-node cluster-state (taskbeat-path storm-id task-id) (Utils/serialize info))
        )

      (remove-task-heartbeat! [this storm-id task-id]
        (delete-node cluster-state (taskbeat-path storm-id task-id))
        )

      (setup-heartbeats! [this storm-id]
        (mkdirs cluster-state (taskbeat-storm-root storm-id)))

      (teardown-heartbeats! [this storm-id]
        (delete-node cluster-state (taskbeat-storm-root storm-id)))

      (teardown-task-errors! [this storm-id]
        (delete-node cluster-state (taskerror-storm-root storm-id)))

      (supervisor-heartbeat! [this supervisor-id info]
        (set-ephemeral-node cluster-state (supervisor-path supervisor-id) (Utils/serialize info))
        )

      (activate-storm! [this storm-id storm-base]
        (set-data cluster-state (storm-path storm-id) (Utils/serialize storm-base))
        )

      (storm-base [this storm-id callback]
        (when callback
          (swap! storm-base-callback assoc storm-id callback))
        (maybe-deserialize (get-data cluster-state (storm-path storm-id) (not-nil? callback)))
        )

      (deactivate-storm! [this storm-id]
        (delete-node cluster-state (storm-path storm-id))
        )

      (set-assignment! [this storm-id info]
        (set-data cluster-state (assignment-path storm-id) (Utils/serialize info))
        )

      (remove-storm! [this storm-id]
        ;; rmr the task related info. must remove assignment last
        (delete-node cluster-state (storm-task-root storm-id))
        (delete-node cluster-state (assignment-path storm-id))
        )

      (report-task-error [this storm-id task-id error]
                         (let [path (taskerror-path storm-id task-id)
                               _ (mkdirs cluster-state path)
                               children (get-children cluster-state path false)
                               times (sort (map #(Integer/parseInt %) children))                               
                               ]
                           (if (>= (count times) 10)
                             (delete-node cluster-state (str path "/" (first times)))
                             )
                           (set-data cluster-state
                                      (str path "/" (current-time-secs))
                                      (.getBytes ^String (stringify-error error)))
                           ))

      (task-errors [this storm-id task-id]
                   (let [path (taskerror-path storm-id task-id)
                         _ (mkdirs cluster-state path)
                         children (get-children cluster-state path false)
                         errors (dofor [c children]
                                       (let [^bytes v (get-data cluster-state (str path "/" c) false)]
                                         (when v
                                           (struct TaskError (String. v) (Integer/parseInt c))
                                           )))
                         ]
                     (->> (filter not-nil? errors)
                          (sort-by :time-secs)
                          )                     
                     ))
      
      (disconnect [this]
        (unregister cluster-state state-id)
        (when solo?
          (close cluster-state)))
      )))

;; daemons have a single thread that will respond to events
;; start with initialize event
;; callbacks add events to the thread's queue

;; keeps in memory cache of the state, only for what client subscribes to. Any subscription is automatically kept in sync, and when there are changes, client is notified.
;; master gives orders through state, and client records status in state (ephemerally)

;; master tells nodes what workers to launch

;; master writes this. supervisors and workers subscribe to this to understand complete topology. each storm is a map from nodes to workers to tasks to ports whenever topology changes everyone will be notified
;; master includes timestamp of each assignment so that appropriate time can be given to each worker to start up
;; /assignments/{storm id}

;; which tasks they talk to, etc. (immutable until shutdown)
;; everyone reads this in full to understand structure
;; /tasks/{storm id}/{task id} ; just contains bolt id


;; supervisors send heartbeats here, master doesn't subscribe but checks asynchronously
;; /supervisors/status/{ephemeral node ids}  ;; node metadata such as port ranges are kept here 

;; tasks send heartbeats here, master doesn't subscribe, just checks asynchronously
;; /taskbeats/{storm id}/{ephemeral task id}

;; contains data about whether it's started or not, tasks and workers subscribe to specific storm here to know when to shutdown
;; master manipulates
;; /storms/{storm id}



;; Zookeeper flows:

;; Master:
;; job submit:
;; 1. read which nodes are available
;; 2. set up the worker/{storm}/{task} stuff (static)
;; 3. set assignments
;; 4. start storm - necessary in case master goes down, when goes back up can remember to take down the storm (2 states: on or off)

;; Monitoring (or by checking when nodes go down or heartbeats aren't received):
;; 1. read assignment
;; 2. see which tasks/nodes are up
;; 3. make new assignment to fix any problems
;; 4. if a storm exists but is not taken down fully, ensure that storm takedown is launched (step by step remove tasks and finally remove assignments)


;; masters only possible watches is on ephemeral nodes and tasks, and maybe not even

;; Supervisor:
;; 1. monitor /storms/* and assignments
;; 2. local state about which workers are local
;; 3. when storm is on, check that workers are running locally & start/kill if different than assignments
;; 4. when storm is off, monitor tasks for workers - when they all die or don't hearbeat, kill the process and cleanup

;; Worker:
;; 1. On startup, start the tasks if the storm is on

;; Task:
;; 1. monitor assignments, reroute when assignments change
;; 2. monitor storm (when storm turns off, error if assignments change) - take down tasks as master turns them off



;; locally on supervisor: workers write pids locally on startup, supervisor deletes it on shutdown (associates pid with worker name)
;; supervisor periodically checks to make sure processes are alive
;; {rootdir}/workers/{storm id}/{worker id}   ;; contains pid inside

;; all tasks in a worker share the same cluster state
;; workers, supervisors, and tasks subscribes to storm to know when it's started or stopped
;; on stopped, master removes records in order (tasks need to subscribe to themselves to see if they disappear)
;; when a master removes a worker, the supervisor should kill it (and escalate to kill -9)
;; on shutdown, tasks subscribe to tasks that send data to them to wait for them to die. when node disappears, they can die
