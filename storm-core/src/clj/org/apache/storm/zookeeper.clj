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

(ns org.apache.storm.zookeeper
  (:import [org.apache.curator.retry RetryNTimes]
           [org.apache.storm Config])
  (:import [org.apache.curator.framework.api CuratorEvent CuratorEventType CuratorListener UnhandledErrorListener])
  (:import [org.apache.curator.framework.state ConnectionStateListener])
  (:import [org.apache.curator.framework CuratorFramework CuratorFrameworkFactory])
  (:import [org.apache.curator.framework.recipes.leader LeaderLatch LeaderLatch$State Participant LeaderLatchListener])
  (:import [org.apache.zookeeper ZooKeeper Watcher KeeperException$NoNodeException
            ZooDefs ZooDefs$Ids CreateMode WatchedEvent Watcher$Event Watcher$Event$KeeperState
            Watcher$Event$EventType KeeperException$NodeExistsException])
  (:import [org.apache.zookeeper.data Stat])
  (:import [org.apache.zookeeper.server ZooKeeperServer NIOServerCnxnFactory])
  (:import [java.net InetSocketAddress BindException InetAddress])
  (:import [org.apache.storm.nimbus ILeaderElector NimbusInfo])
  (:import [java.io File])
  (:import [java.util List Map])
  (:import [org.apache.storm.zookeeper Zookeeper ZkKeeperStates ZkEventTypes])
  (:import [org.apache.storm.utils Utils ZookeeperAuthInfo])
  (:use [org.apache.storm util log config]))


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
                (watcher (.getState event)
                  (.getType event)
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
