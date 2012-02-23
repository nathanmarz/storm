(ns backtype.storm.zookeeper
  (:import [com.netflix.curator.retry RetryNTimes])
  (:import [com.netflix.curator.framework.api CuratorEvent CuratorEventType CuratorListener UnhandledErrorListener])
  (:import [com.netflix.curator.framework CuratorFramework CuratorFrameworkFactory])
  (:import [org.apache.zookeeper ZooKeeper Watcher KeeperException$NoNodeException
            ZooDefs ZooDefs$Ids CreateMode WatchedEvent Watcher$Event Watcher$Event$KeeperState
            Watcher$Event$EventType KeeperException$NodeExistsException])
  (:import [org.apache.zookeeper.data Stat])
  (:import [org.apache.zookeeper.server ZooKeeperServer NIOServerCnxn$Factory])
  (:import [java.net InetSocketAddress])
  (:import [java.io File])
  (:import [backtype.storm.utils Utils])
  (:use [backtype.storm util log config])
  (:use [clojure.contrib.def :only [defnk]]))

(def zk-keeper-states
  {Watcher$Event$KeeperState/Disconnected :disconnected
   Watcher$Event$KeeperState/SyncConnected :connected
   Watcher$Event$KeeperState/AuthFailed :auth-failed
   Watcher$Event$KeeperState/Expired :expired
  })

(def zk-event-types
  {Watcher$Event$EventType/None :none
   Watcher$Event$EventType/NodeCreated :node-created
   Watcher$Event$EventType/NodeDeleted :node-deleted
   Watcher$Event$EventType/NodeDataChanged :node-data-changed
   Watcher$Event$EventType/NodeChildrenChanged :node-children-changed
  })

(defn- default-watcher [state type path]
  (log-message "Zookeeper state update: " state type path))

(defnk mk-client [conf servers port :root "" :watcher default-watcher]
  (let [fk (Utils/newCurator conf servers port root)]
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
    (.. fk
        (getUnhandledErrorListenable)
        (addListener
         (reify UnhandledErrorListener
           (unhandledError [this msg error]
             (log-error error "Unrecoverable Zookeeper error, halting process: " msg)
             (halt-process! 1 "Unrecoverable Zookeeper error")))))
    (.start fk)
    fk))

(def zk-create-modes
  {:ephemeral CreateMode/EPHEMERAL
   :persistent CreateMode/PERSISTENT})

(defn create-node
  ([^CuratorFramework zk ^String path ^bytes data mode]
    (.. zk (create) (withMode (zk-create-modes mode)) (withACL ZooDefs$Ids/OPEN_ACL_UNSAFE) (forPath (normalize-path path) data)))
  ([^CuratorFramework zk ^String path ^bytes data]
    (create-node zk path data :persistent)))

(defn exists-node? [^CuratorFramework zk ^String path watch?]
  ((complement nil?)
    (if watch?
       (.. zk (checkExists) (watched) (forPath (normalize-path path))) 
       (.. zk (checkExists) (forPath (normalize-path path))))))

(defn delete-node [^CuratorFramework zk ^String path]
  (.. zk (delete) (forPath (normalize-path path))))

(defn mkdirs [^CuratorFramework zk ^String path]
  (let [path (normalize-path path)]
    (when-not (or (= path "/") (exists-node? zk path false))
      (mkdirs zk (parent-path path))
      (try-cause
        (create-node zk path (barr 7) :persistent)
        (catch KeeperException$NodeExistsException e
          ;; this can happen when multiple clients doing mkdir at same time
          ))
      )))

(defn get-data [^CuratorFramework zk ^String path watch?]
  (let [path (normalize-path path)]
    (try-cause
      (if (exists-node? zk path watch?)
        (if watch?
          (.. zk (getData) (watched) (forPath path))
          (.. zk (getData) (forPath path))))
    (catch KeeperException$NoNodeException e
      ;; this is fine b/c we still have a watch from the successful exists call
      nil ))))

(defn get-children [^CuratorFramework zk ^String path watch?]
  (if watch?
    (.. zk (getChildren) (watched) (forPath (normalize-path path)))
    (.. zk (getChildren) (forPath (normalize-path path)))))

(defn set-data [^CuratorFramework zk ^String path ^bytes data]
  (.. zk (setData) (forPath (normalize-path path) data)))

(defn exists [^CuratorFramework zk ^String path watch?]
  (exists-node? zk path watch?))

(defn delete-recursive [^CuratorFramework zk ^String path]
  (let [path (normalize-path path)]
    (when (exists-node? zk path false)
      (let [children (try-cause (get-children zk path false)
                                (catch KeeperException$NoNodeException e
                                  []
                                  ))]
        (doseq [c children]
          (delete-recursive zk (full-path path c)))
        (try-cause (delete-node zk path)
                   (catch KeeperException$NoNodeException e
                                  nil
                                  ))
        ))))

(defn mk-inprocess-zookeeper [localdir port]
  (log-message "Starting inprocess zookeeper at port " port " and dir " localdir)
  (let [localfile (File. localdir)
        zk (ZooKeeperServer. localfile localfile 2000)
        factory (NIOServerCnxn$Factory. (InetSocketAddress. port))]
    (.startup factory zk)
    factory
    ))

(defn shutdown-inprocess-zookeeper [handle]
  (.shutdown handle))
