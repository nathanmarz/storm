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

(ns backtype.storm.zookeeper
  (:import [org.apache.curator.retry RetryNTimes]
           [backtype.storm Config])
  (:import [org.apache.curator.framework.api CuratorEvent CuratorEventType CuratorListener UnhandledErrorListener])
  (:import [org.apache.curator.framework CuratorFramework CuratorFrameworkFactory])
  (:import [org.apache.curator.framework.recipes.leader LeaderLatch LeaderLatch$State Participant LeaderLatchListener])
  (:import [org.apache.zookeeper ZooKeeper Watcher KeeperException$NoNodeException
            ZooDefs ZooDefs$Ids CreateMode WatchedEvent Watcher$Event Watcher$Event$KeeperState
            Watcher$Event$EventType KeeperException$NodeExistsException])
  (:import [org.apache.zookeeper.data Stat])
  (:import [org.apache.zookeeper.server ZooKeeperServer NIOServerCnxnFactory])
  (:import [java.net InetSocketAddress BindException InetAddress])
  (:import [backtype.storm.nimbus ILeaderElector NimbusInfo])
  (:import [java.io File])
  (:import [java.util List Map])
  (:import [backtype.storm.utils Utils ZookeeperAuthInfo])
  (:use [backtype.storm util log config]))

(def zk-keeper-states
  {Watcher$Event$KeeperState/Disconnected :disconnected
   Watcher$Event$KeeperState/SyncConnected :connected
   Watcher$Event$KeeperState/AuthFailed :auth-failed
   Watcher$Event$KeeperState/Expired :expired})

(def zk-event-types
  {Watcher$Event$EventType/None :none
   Watcher$Event$EventType/NodeCreated :node-created
   Watcher$Event$EventType/NodeDeleted :node-deleted
   Watcher$Event$EventType/NodeDataChanged :node-data-changed
   Watcher$Event$EventType/NodeChildrenChanged :node-children-changed})

(defn- default-watcher
  [state type path]
  (log-message "Zookeeper state update: " state type path))

(defnk mk-client
  [conf servers port
   :root ""
   :watcher default-watcher
   :auth-conf nil]
  (let [fk (Utils/newCurator conf servers port root (when auth-conf (ZookeeperAuthInfo. auth-conf)))]
    (.. fk
        (getCuratorListenable)
        (addListener
          (reify CuratorListener
            (^void eventReceived [this ^CuratorFramework _fk ^CuratorEvent e]
                   (when (= (.getType e) CuratorEventType/WATCHED)
                     (let [^WatchedEvent event (.getWatchedEvent e)]
                       (watcher (zk-keeper-states (.getState event))
                                (zk-event-types (.getType event))
                                (.getPath event))))))))
    ;;    (.. fk
    ;;        (getUnhandledErrorListenable)
    ;;        (addListener
    ;;         (reify UnhandledErrorListener
    ;;           (unhandledError [this msg error]
    ;;             (if (or (exception-cause? InterruptedException error)
    ;;                     (exception-cause? java.nio.channels.ClosedByInterruptException error))
    ;;               (do (log-warn-error error "Zookeeper exception " msg)
    ;;                   (let [to-throw (InterruptedException.)]
    ;;                     (.initCause to-throw error)
    ;;                     (throw to-throw)
    ;;                     ))
    ;;               (do (log-error error "Unrecoverable Zookeeper error " msg)
    ;;                   (halt-process! 1 "Unrecoverable Zookeeper error")))
    ;;             ))))
    (.start fk)
    fk))

(def zk-create-modes
  {:ephemeral CreateMode/EPHEMERAL
   :persistent CreateMode/PERSISTENT
   :sequential CreateMode/PERSISTENT_SEQUENTIAL})

(defn create-node
  ([^CuratorFramework zk ^String path ^bytes data mode]
   (try
     (.. zk (create) (withMode (zk-create-modes mode)) (withACL ZooDefs$Ids/OPEN_ACL_UNSAFE) (forPath (normalize-path path) data))
     (catch Exception e (throw (wrap-in-runtime e)))))
  ([^CuratorFramework zk ^String path ^bytes data]
   (create-node zk path data :persistent)))

(defn exists-node?
  [^CuratorFramework zk ^String path watch?]
  ((complement nil?)
   (try
     (if watch?
       (.. zk (checkExists) (watched) (forPath (normalize-path path)))
       (.. zk (checkExists) (forPath (normalize-path path))))
     (catch Exception e (throw (wrap-in-runtime e))))))

(defnk delete-node
  [^CuratorFramework zk ^String path :force false]
  (try-cause  (.. zk (delete) (forPath (normalize-path path)))
             (catch KeeperException$NoNodeException e
               (when-not force (throw e)))
             (catch Exception e (throw (wrap-in-runtime e)))))

(defn mkdirs
  [^CuratorFramework zk ^String path]
  (let [path (normalize-path path)]
    (when-not (or (= path "/") (exists-node? zk path false))
      (mkdirs zk (parent-path path))
      (try-cause
        (create-node zk path (barr 7) :persistent)
        (catch KeeperException$NodeExistsException e
          ;; this can happen when multiple clients doing mkdir at same time
          ))
      )))

(defn get-data
  [^CuratorFramework zk ^String path watch?]
  (let [path (normalize-path path)]
    (try-cause
      (if (exists-node? zk path watch?)
        (if watch?
          (.. zk (getData) (watched) (forPath path))
          (.. zk (getData) (forPath path))))
      (catch KeeperException$NoNodeException e
        ;; this is fine b/c we still have a watch from the successful exists call
        nil )
      (catch Exception e (throw (wrap-in-runtime e))))))

(defn get-data-with-version 
  [^CuratorFramework zk ^String path watch?]
  (let [stats (org.apache.zookeeper.data.Stat. )
        path (normalize-path path)]
    (try-cause
     (if-let [data 
              (if (exists-node? zk path watch?)
                (if watch?
                  (.. zk (getData) (watched) (storingStatIn stats) (forPath path))
                  (.. zk (getData) (storingStatIn stats) (forPath path))))]
       {:data data
        :version (.getVersion stats)})
     (catch KeeperException$NoNodeException e
       ;; this is fine b/c we still have a watch from the successful exists call
       nil ))))

(defn get-version 
[^CuratorFramework zk ^String path watch?]
  (if-let [stats
           (if watch?
             (.. zk (checkExists) (watched) (forPath (normalize-path path)))
             (.. zk (checkExists) (forPath (normalize-path path))))]
    (.getVersion stats)
    nil))

(defn get-children
  [^CuratorFramework zk ^String path watch?]
  (try
    (if watch?
      (.. zk (getChildren) (watched) (forPath (normalize-path path)))
      (.. zk (getChildren) (forPath (normalize-path path))))
    (catch Exception e (throw (wrap-in-runtime e)))))

(defn set-data
  [^CuratorFramework zk ^String path ^bytes data]
  (try
    (.. zk (setData) (forPath (normalize-path path) data))
    (catch Exception e (throw (wrap-in-runtime e)))))

(defn exists
  [^CuratorFramework zk ^String path watch?]
  (exists-node? zk path watch?))

(defn delete-recursive
  [^CuratorFramework zk ^String path]
  (let [path (normalize-path path)]
    (when (exists-node? zk path false)
      (let [children (try-cause
                       (get-children zk path false)
                       (catch KeeperException$NoNodeException e []))]
        (doseq [c children]
          (delete-recursive zk (full-path path c)))
        (delete-node zk path :force true)))))

(defnk mk-inprocess-zookeeper
  [localdir :port nil]
  (let [localfile (File. localdir)
        zk (ZooKeeperServer. localfile localfile 2000)
        [retport factory]
        (loop [retport (if port port 2000)]
          (if-let [factory-tmp
                   (try-cause
                     (doto (NIOServerCnxnFactory.)
                       (.configure (InetSocketAddress. retport) 0))
                     (catch BindException e
                       (when (> (inc retport) (if port port 65535))
                         (throw (RuntimeException.
                                  "No port is available to launch an inprocess zookeeper.")))))]
            [retport factory-tmp]
            (recur (inc retport))))]
    (log-message "Starting inprocess zookeeper at port " retport " and dir " localdir)
    (.startup factory zk)
    [retport factory]))

(defn shutdown-inprocess-zookeeper
  [handle]
  (.shutdown handle))

(defn- to-NimbusInfo [^Participant participant]
  (let
    [id (if (clojure.string/blank? (.getId participant))
          (throw (RuntimeException. "No nimbus leader participant host found, have you started your nimbus hosts?"))
          (.getId participant))
     server (first (.split id ":"))
     port (Integer/parseInt (last (.split id ":")))
     is-leader (.isLeader participant)]
    (NimbusInfo. server port is-leader)))

(defn leader-latch-listener-impl
  "Leader latch listener that will be invoked when we either gain or lose leadership"
  [conf zk leader-latch]
  (let [hostname (.getCanonicalHostName (InetAddress/getLocalHost))
        STORMS-ROOT (str (conf STORM-ZOOKEEPER-ROOT) "/storms")]
    (reify LeaderLatchListener
      (^void isLeader[this]
        (log-message (str hostname "gained leadership, checking if it has all the topology code locally."))
        (let [active-topology-ids (set (get-children zk STORMS-ROOT false))
              local-topology-ids (set (.list (File. (master-stormdist-root conf))))
              diff-topology (first (set-delta active-topology-ids local-topology-ids))]
        (log-message "active-topology-ids [" (clojure.string/join "," active-topology-ids)
                          "] local-topology-ids [" (clojure.string/join "," local-topology-ids)
                          "] diff-topology [" (clojure.string/join "," diff-topology) "]")
        (if (empty? diff-topology)
          (log-message " Accepting leadership, all active topology found localy.")
          (do
            (log-message " code for all active topologies not available locally, giving up leadership.")
            (.close leader-latch)))))
      (^void notLeader[this]
        (log-message (str hostname " lost leadership."))))))

(defn zk-leader-elector
  "Zookeeper Implementation of ILeaderElector."
  [conf]
  (let [servers (conf STORM-ZOOKEEPER-SERVERS)
        zk (mk-client conf (conf STORM-ZOOKEEPER-SERVERS) (conf STORM-ZOOKEEPER-PORT) :auth-conf conf)
        leader-lock-path (str (conf STORM-ZOOKEEPER-ROOT) "/leader-lock")
        id (str (.getCanonicalHostName (InetAddress/getLocalHost)) ":" (conf NIMBUS-THRIFT-PORT))
        leader-latch (atom (LeaderLatch. zk leader-lock-path id))
        leader-latch-listener (atom (leader-latch-listener-impl conf zk @leader-latch))
        ]
    (reify ILeaderElector
      (prepare [this conf]
        (log-message "no-op for zookeeper implementation"))

      (^void addToLeaderLockQueue [this]
        (let [state (.getState @leader-latch)]
        ;if this latch is already closed, we need to create new instance.
        (if (.equals LeaderLatch$State/CLOSED state)
          (do
            (reset! leader-latch (LeaderLatch. zk leader-lock-path id))
            (reset! leader-latch-listener (leader-latch-listener-impl conf zk @leader-latch))
            (log-message "LeaderLatch was in closed state. Resetted the leaderLatch and listeners.")
            ))

        ;Only if the latch is not already started we invoke start.
        (if (.equals LeaderLatch$State/LATENT state)
          (do
            (.addListener @leader-latch @leader-latch-listener)
            (.start @leader-latch)
            (log-message "Queued up for leader lock."))
          (log-message "Node already in queue for leader lock."))))

      (^void removeFromLeaderLockQueue [this]
        ;Only started latches can be closed.
        (if (.equals LeaderLatch$State/STARTED (.getState @leader-latch))
          (do
            (.close @leader-latch)
            (log-message "Removed from leader lock queue."))
          (log-message "leader latch is not started so no removeFromLeaderLockQueue needed.")))

      (^boolean isLeader [this]
        (.hasLeadership @leader-latch))

      (^NimbusInfo getLeader [this]
        (to-NimbusInfo (.getLeader @leader-latch)))

      (^List getAllNimbuses [this]
        (let [participants (.getParticipants @leader-latch)]
          (map (fn [^Participant participant]
                 (to-NimbusInfo participant))
            participants)))

      (^void close[this]
        (log-message "closing zookeeper connection of leader elector.")
        (.close zk)))))