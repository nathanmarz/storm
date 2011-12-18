(ns backtype.storm.zookeeper
  (:import [org.apache.zookeeper ZooKeeper Watcher KeeperException$NoNodeException
            ZooDefs ZooDefs$Ids CreateMode WatchedEvent Watcher$Event Watcher$Event$KeeperState
            Watcher$Event$EventType KeeperException$NodeExistsException])
  (:import [org.apache.zookeeper.data Stat])
  (:import [org.apache.zookeeper.server ZooKeeperServer NIOServerCnxn$Factory])
  (:import [java.net InetSocketAddress])
  (:import [java.io File])
  (:use [backtype.storm util log config]))

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


;; TODO: make this block until session is established (wait until a flag is triggered by watcher)
(defn mk-client
  ([conn-str session-timeout watcher]
    (ZooKeeper.
      conn-str
      session-timeout
      (reify Watcher
        (^void process [this ^WatchedEvent event]
          (watcher (zk-keeper-states (.getState event))
                   (zk-event-types (.getType event))
                   (.getPath event))
          ))))
  ([conn-str watcher]
    (mk-client conn-str 10000 watcher))
  ([conn-str]
    ;; this constructor is intended for debugging
    (mk-client
      conn-str
      (fn [state type path]
        (log-message "Zookeeper state update: " state type path)))
      ))

(def zk-create-modes
  {:ephemeral CreateMode/EPHEMERAL
   :persistent CreateMode/PERSISTENT})

(defn create-node
  ([^ZooKeeper zk ^String path ^bytes data mode]
    (.create zk (normalize-path path) data ZooDefs$Ids/OPEN_ACL_UNSAFE (zk-create-modes mode)))
  ([^ZooKeeper zk ^String path ^bytes data]
    (create-node zk path data :persistent)))

(defn exists-node? [^ZooKeeper zk ^String path watch?]
  ((complement nil?) (.exists zk (normalize-path path) watch?)))

(defn delete-node [^ZooKeeper zk ^String path]
  (.delete zk (normalize-path path) -1))

(defn mkdirs [^ZooKeeper zk ^String path]
  (let [path (normalize-path path)]
    (when-not (or (= path "/") (exists-node? zk path false))
      (mkdirs zk (parent-path path))
      (try
        (create-node zk path (barr 7) :persistent)
        (catch KeeperException$NodeExistsException e
          ;; this can happen when multiple clients doing mkdir at same time
          ))
      )))

(defn get-data [^ZooKeeper zk ^String path watch?]
  (let [path (normalize-path path)]
    (try
      (if (.exists zk path watch?)
        (.getData zk path watch? (Stat.)))
    (catch KeeperException$NoNodeException e
      ;; this is fine b/c we still have a watch from the successful exists call
      nil ))))

(defn get-children [^ZooKeeper zk ^String path watch?]
  (.getChildren zk (normalize-path path) watch?)
  )

(defn set-data [^ZooKeeper zk ^String path ^bytes data]
  (.setData zk (normalize-path path) data -1))

(defn exists [^ZooKeeper zk ^String path watch?]
  (.exists zk (normalize-path path) watch?)
  )

(defn delete-recursive [^ZooKeeper zk ^String path]
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
