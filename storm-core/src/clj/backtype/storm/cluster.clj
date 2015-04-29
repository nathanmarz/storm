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

(ns backtype.storm.cluster
  (:import [org.apache.zookeeper.data Stat ACL Id]
           [backtype.storm.generated SupervisorInfo Assignment StormBase ClusterWorkerHeartbeat ErrorInfo Credentials]
           [java.io Serializable])
  (:import [org.apache.zookeeper KeeperException KeeperException$NoNodeException ZooDefs ZooDefs$Ids ZooDefs$Perms])
  (:import [backtype.storm.utils Utils])
  (:import [java.security MessageDigest])
  (:import [org.apache.zookeeper.server.auth DigestAuthenticationProvider])
  (:use [backtype.storm util log config converter])
  (:require [backtype.storm [zookeeper :as zk]])
  (:require [backtype.storm.daemon [common :as common]]))

(defprotocol ClusterState
  (set-ephemeral-node [this path data acls])
  (delete-node [this path])
  (create-sequential [this path data acls])
  ;; if node does not exist, create persistent with this data
  (set-data [this path data acls])
  (get-data [this path watch?])
  (get-version [this path watch?])
  (get-data-with-version [this path watch?])
  (get-children [this path watch?])
  (mkdirs [this path acls])
  (exists-node? [this path watch?])
  (close [this])
  (register [this callback])
  (unregister [this id]))

(defn mk-topo-only-acls
  [topo-conf]
  (let [payload (.get topo-conf STORM-ZOOKEEPER-TOPOLOGY-AUTH-PAYLOAD)]
    (when (Utils/isZkAuthenticationConfiguredTopology topo-conf)
      [(first ZooDefs$Ids/CREATOR_ALL_ACL)
       (ACL. ZooDefs$Perms/READ (Id. "digest" (DigestAuthenticationProvider/generateDigest payload)))])))

(defnk mk-distributed-cluster-state
  [conf :auth-conf nil :acls nil]
  (let [zk (zk/mk-client conf (conf STORM-ZOOKEEPER-SERVERS) (conf STORM-ZOOKEEPER-PORT) :auth-conf auth-conf)]
    (zk/mkdirs zk (conf STORM-ZOOKEEPER-ROOT) acls)
    (.close zk))
  (let [callbacks (atom {})
        active (atom true)
        zk (zk/mk-client conf
                         (conf STORM-ZOOKEEPER-SERVERS)
                         (conf STORM-ZOOKEEPER-PORT)
                         :auth-conf auth-conf
                         :root (conf STORM-ZOOKEEPER-ROOT)
                         :watcher (fn [state type path]
                                    (when @active
                                      (when-not (= :connected state)
                                        (log-warn "Received event " state ":" type ":" path " with disconnected Zookeeper."))
                                      (when-not (= :none type)
                                        (doseq [callback (vals @callbacks)]
                                          (callback type path))))))]
    (reify
     ClusterState

     (register
       [this callback]
       (let [id (uuid)]
         (swap! callbacks assoc id callback)
         id))

     (unregister
       [this id]
       (swap! callbacks dissoc id))

     (set-ephemeral-node
       [this path data acls]
       (zk/mkdirs zk (parent-path path) acls)
       (if (zk/exists zk path false)
         (try-cause
           (zk/set-data zk path data) ; should verify that it's ephemeral
           (catch KeeperException$NoNodeException e
             (log-warn-error e "Ephemeral node disappeared between checking for existing and setting data")
             (zk/create-node zk path data :ephemeral acls)))
         (zk/create-node zk path data :ephemeral acls)))

     (create-sequential
       [this path data acls]
       (zk/create-node zk path data :sequential acls))

     (set-data
       [this path data acls]
       ;; note: this does not turn off any existing watches
       (if (zk/exists zk path false)
         (zk/set-data zk path data)
         (do
           (zk/mkdirs zk (parent-path path) acls)
           (zk/create-node zk path data :persistent acls))))

     (delete-node
       [this path]
       (zk/delete-recursive zk path))

     (get-data
       [this path watch?]
       (zk/get-data zk path watch?))

     (get-data-with-version
       [this path watch?]
       (zk/get-data-with-version zk path watch?))

     (get-version 
       [this path watch?]
       (zk/get-version zk path watch?))

     (get-children
       [this path watch?]
       (zk/get-children zk path watch?))

     (mkdirs
       [this path acls]
       (zk/mkdirs zk path acls))

     (exists-node?
       [this path watch?]
       (zk/exists-node? zk path watch?))

     (close
       [this]
       (reset! active false)
       (.close zk)))))

(defprotocol StormClusterState
  (assignments [this callback])
  (assignment-info [this storm-id callback])
  (assignment-info-with-version [this storm-id callback])
  (assignment-version [this storm-id callback])
  (active-storms [this])
  (storm-base [this storm-id callback])
  (get-worker-heartbeat [this storm-id node port])
  (executor-beats [this storm-id executor->node+port])
  (supervisors [this callback])
  (supervisor-info [this supervisor-id]) ;; returns nil if doesn't exist
  (setup-heartbeats! [this storm-id])
  (teardown-heartbeats! [this storm-id])
  (teardown-topology-errors! [this storm-id])
  (heartbeat-storms [this])
  (error-topologies [this])
  (worker-heartbeat! [this storm-id node port info])
  (remove-worker-heartbeat! [this storm-id node port])
  (supervisor-heartbeat! [this supervisor-id info])
  (activate-storm! [this storm-id storm-base])
  (update-storm! [this storm-id new-elems])
  (remove-storm-base! [this storm-id])
  (set-assignment! [this storm-id info])
  (remove-storm! [this storm-id])
  (report-error [this storm-id task-id node port error])
  (errors [this storm-id task-id])
  (last-error [this storm-id task-id])
  (set-credentials! [this storm-id creds topo-conf])
  (credentials [this storm-id callback])
  (disconnect [this]))

(def ASSIGNMENTS-ROOT "assignments")
(def CODE-ROOT "code")
(def STORMS-ROOT "storms")
(def SUPERVISORS-ROOT "supervisors")
(def WORKERBEATS-ROOT "workerbeats")
(def ERRORS-ROOT "errors")
(def CREDENTIALS-ROOT "credentials")

(def ASSIGNMENTS-SUBTREE (str "/" ASSIGNMENTS-ROOT))
(def STORMS-SUBTREE (str "/" STORMS-ROOT))
(def SUPERVISORS-SUBTREE (str "/" SUPERVISORS-ROOT))
(def WORKERBEATS-SUBTREE (str "/" WORKERBEATS-ROOT))
(def ERRORS-SUBTREE (str "/" ERRORS-ROOT))
(def CREDENTIALS-SUBTREE (str "/" CREDENTIALS-ROOT))

(defn supervisor-path
  [id]
  (str SUPERVISORS-SUBTREE "/" id))

(defn assignment-path
  [id]
  (str ASSIGNMENTS-SUBTREE "/" id))

(defn storm-path
  [id]
  (str STORMS-SUBTREE "/" id))

(defn workerbeat-storm-root
  [storm-id]
  (str WORKERBEATS-SUBTREE "/" storm-id))

(defn workerbeat-path
  [storm-id node port]
  (str (workerbeat-storm-root storm-id) "/" node "-" port))

(defn error-storm-root
  [storm-id]
  (str ERRORS-SUBTREE "/" storm-id))

(defn error-path
  [storm-id component-id]
  (str (error-storm-root storm-id) "/" (url-encode component-id)))

(def last-error-path-seg "last-error")

(defn last-error-path
  [storm-id component-id]
  (str (error-storm-root storm-id)
       "/"
       (url-encode component-id)
       "-"
       last-error-path-seg))

(defn credentials-path
  [storm-id]
  (str CREDENTIALS-SUBTREE "/" storm-id))

(defn- issue-callback!
  [cb-atom]
  (let [cb @cb-atom]
    (reset! cb-atom nil)
    (when cb
      (cb))))

(defn- issue-map-callback!
  [cb-atom id]
  (let [cb (@cb-atom id)]
    (swap! cb-atom dissoc id)
    (when cb
      (cb id))))

(defn- maybe-deserialize
  [ser clazz]
  (when ser
    (Utils/deserialize ser clazz)))

(defrecord TaskError [error time-secs host port])

(defn- parse-error-path
  [^String p]
  (Long/parseLong (.substring p 1)))

(defn convert-executor-beats
  "Ensures that we only return heartbeats for executors assigned to
  this worker."
  [executors worker-hb]
  (let [executor-stats (:executor-stats worker-hb)]
    (->> executors
         (map (fn [t]
                (if (contains? executor-stats t)
                  {t {:time-secs (:time-secs worker-hb)
                      :uptime (:uptime worker-hb)
                      :stats (get executor-stats t)}})))
         (into {}))))

;; Watches should be used for optimization. When ZK is reconnecting, they're not guaranteed to be called.
(defnk mk-storm-cluster-state
  [cluster-state-spec :acls nil]
  (let [[solo? cluster-state] (if (satisfies? ClusterState cluster-state-spec)
                                [false cluster-state-spec]
                                [true (mk-distributed-cluster-state cluster-state-spec :auth-conf cluster-state-spec :acls acls)])
        assignment-info-callback (atom {})
        assignment-info-with-version-callback (atom {})
        assignment-version-callback (atom {})
        supervisors-callback (atom nil)
        assignments-callback (atom nil)
        storm-base-callback (atom {})
        credentials-callback (atom {})
        state-id (register
                  cluster-state
                  (fn [type path]
                    (let [[subtree & args] (tokenize-path path)]
                      (condp = subtree
                         ASSIGNMENTS-ROOT (if (empty? args)
                                             (issue-callback! assignments-callback)
                                             (do
                                               (issue-map-callback! assignment-info-callback (first args))
                                               (issue-map-callback! assignment-version-callback (first args))
                                               (issue-map-callback! assignment-info-with-version-callback (first args))))
                         SUPERVISORS-ROOT (issue-callback! supervisors-callback)
                         STORMS-ROOT (issue-map-callback! storm-base-callback (first args))
                         CREDENTIALS-ROOT (issue-map-callback! credentials-callback (first args))
                         ;; this should never happen
                         (exit-process! 30 "Unknown callback for subtree " subtree args)))))]
    (doseq [p [ASSIGNMENTS-SUBTREE STORMS-SUBTREE SUPERVISORS-SUBTREE WORKERBEATS-SUBTREE ERRORS-SUBTREE]]
      (mkdirs cluster-state p acls))
    (reify
      StormClusterState

      (assignments
        [this callback]
        (when callback
          (reset! assignments-callback callback))
        (get-children cluster-state ASSIGNMENTS-SUBTREE (not-nil? callback)))

      (assignment-info
        [this storm-id callback]
        (when callback
          (swap! assignment-info-callback assoc storm-id callback))
        (clojurify-assignment (maybe-deserialize (get-data cluster-state (assignment-path storm-id) (not-nil? callback)) Assignment)))

      (assignment-info-with-version 
        [this storm-id callback]
        (when callback
          (swap! assignment-info-with-version-callback assoc storm-id callback))
        (let [{data :data version :version} 
              (get-data-with-version cluster-state (assignment-path storm-id) (not-nil? callback))]
        {:data (clojurify-assignment (maybe-deserialize data Assignment))
         :version version}))

      (assignment-version 
        [this storm-id callback]
        (when callback
          (swap! assignment-version-callback assoc storm-id callback))
        (get-version cluster-state (assignment-path storm-id) (not-nil? callback)))

      (active-storms
        [this]
        (get-children cluster-state STORMS-SUBTREE false))

      (heartbeat-storms
        [this]
        (get-children cluster-state WORKERBEATS-SUBTREE false))

      (error-topologies
        [this]
        (get-children cluster-state ERRORS-SUBTREE false))

      (get-worker-heartbeat
        [this storm-id node port]
        (let [worker-hb (get-data cluster-state (workerbeat-path storm-id node port) false)]
          (if worker-hb
            (-> worker-hb
              (maybe-deserialize ClusterWorkerHeartbeat)
              clojurify-zk-worker-hb))))


      (executor-beats
        [this storm-id executor->node+port]
        ;; need to take executor->node+port in explicitly so that we don't run into a situation where a
        ;; long dead worker with a skewed clock overrides all the timestamps. By only checking heartbeats
        ;; with an assigned node+port, and only reading executors from that heartbeat that are actually assigned,
        ;; we avoid situations like that
        (let [node+port->executors (reverse-map executor->node+port)
              all-heartbeats (for [[[node port] executors] node+port->executors]
                               (->> (get-worker-heartbeat this storm-id node port)
                                    (convert-executor-beats executors)
                                    ))]
          (apply merge all-heartbeats)))

      (supervisors
        [this callback]
        (when callback
          (reset! supervisors-callback callback))
        (get-children cluster-state SUPERVISORS-SUBTREE (not-nil? callback)))

      (supervisor-info
        [this supervisor-id]
        (clojurify-supervisor-info (maybe-deserialize (get-data cluster-state (supervisor-path supervisor-id) false) SupervisorInfo)))

      (worker-heartbeat!
        [this storm-id node port info]
        (let [thrift-worker-hb (thriftify-zk-worker-hb info)]
          (if thrift-worker-hb
            (set-data cluster-state (workerbeat-path storm-id node port) (Utils/serialize thrift-worker-hb) acls))))

      (remove-worker-heartbeat!
        [this storm-id node port]
        (delete-node cluster-state (workerbeat-path storm-id node port)))

      (setup-heartbeats!
        [this storm-id]
        (mkdirs cluster-state (workerbeat-storm-root storm-id) acls))

      (teardown-heartbeats!
        [this storm-id]
        (try-cause
          (delete-node cluster-state (workerbeat-storm-root storm-id))
          (catch KeeperException e
            (log-warn-error e "Could not teardown heartbeats for " storm-id))))

      (teardown-topology-errors!
        [this storm-id]
        (try-cause
          (delete-node cluster-state (error-storm-root storm-id))
          (catch KeeperException e
            (log-warn-error e "Could not teardown errors for " storm-id))))

      (supervisor-heartbeat!
        [this supervisor-id info]
        (let [thrift-supervisor-info (thriftify-supervisor-info info)]
          (set-ephemeral-node cluster-state (supervisor-path supervisor-id) (Utils/serialize thrift-supervisor-info) acls)))

      (activate-storm!
        [this storm-id storm-base]
        (let [thrift-storm-base (thriftify-storm-base storm-base)]
          (set-data cluster-state (storm-path storm-id) (Utils/serialize thrift-storm-base) acls)))

      (update-storm!
        [this storm-id new-elems]
        (let [base (storm-base this storm-id nil)
              executors (:component->executors base)
              new-elems (update new-elems :component->executors (partial merge executors))]
          (set-data cluster-state (storm-path storm-id)
                    (-> base
                        (merge new-elems)
                        thriftify-storm-base
                        Utils/serialize)
                    acls)))

      (storm-base
        [this storm-id callback]
        (when callback
          (swap! storm-base-callback assoc storm-id callback))
        (clojurify-storm-base (maybe-deserialize (get-data cluster-state (storm-path storm-id) (not-nil? callback)) StormBase)))

      (remove-storm-base!
        [this storm-id]
        (delete-node cluster-state (storm-path storm-id)))

      (set-assignment!
        [this storm-id info]
        (let [thrift-assignment (thriftify-assignment info)]
          (set-data cluster-state (assignment-path storm-id) (Utils/serialize thrift-assignment) acls)))

      (remove-storm!
        [this storm-id]
        (delete-node cluster-state (assignment-path storm-id))
        (delete-node cluster-state (credentials-path storm-id))
        (remove-storm-base! this storm-id))

      (set-credentials!
         [this storm-id creds topo-conf]
         (let [topo-acls (mk-topo-only-acls topo-conf)
               path (credentials-path storm-id)
               thriftified-creds (thriftify-credentials creds)]
           (set-data cluster-state path (Utils/serialize thriftified-creds) topo-acls)))

      (credentials
        [this storm-id callback]
        (when callback
          (swap! credentials-callback assoc storm-id callback))
        (clojurify-crdentials (maybe-deserialize (get-data cluster-state (credentials-path storm-id) (not-nil? callback)) Credentials)))

      (report-error
         [this storm-id component-id node port error]
         (let [path (error-path storm-id component-id)
               last-error-path (last-error-path storm-id component-id)
               data (thriftify-error {:time-secs (current-time-secs) :error (stringify-error error) :host node :port port})
               _ (mkdirs cluster-state path acls)
               ser-data (Utils/serialize data)
               _ (mkdirs cluster-state path acls)
               _ (create-sequential cluster-state (str path "/e") ser-data acls)
               _ (set-data cluster-state last-error-path ser-data acls)
               to-kill (->> (get-children cluster-state path false)
                            (sort-by parse-error-path)
                            reverse
                            (drop 10))]
           (doseq [k to-kill]
             (delete-node cluster-state (str path "/" k)))))

      (errors
         [this storm-id component-id]
         (let [path (error-path storm-id component-id)
               errors (if (exists-node? cluster-state path false)
                        (dofor [c (get-children cluster-state path false)]
                          (if-let [data (-> (get-data cluster-state
                                                      (str path "/" c)
                                                      false)
                                          (maybe-deserialize ErrorInfo)
                                          clojurify-error)]
                            (map->TaskError data)))
                        ())]
           (->> (filter not-nil? errors)
                (sort-by (comp - :time-secs)))))

      (last-error
        [this storm-id component-id]
        (let [path (last-error-path storm-id component-id)]
          (if (exists-node? cluster-state path false)
            (if-let [data (-> (get-data cluster-state path false)
                              (maybe-deserialize ErrorInfo)
                              clojurify-error)]
              (map->TaskError data)))))
      
      (disconnect
         [this]
        (unregister cluster-state state-id)
        (when solo?
          (close cluster-state))))))

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
