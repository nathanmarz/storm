/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.zookeeper;

import clojure.lang.PersistentArrayMap;
import clojure.lang.RT;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.storm.Config;
import org.apache.storm.callback.DefaultWatcherCallBack;
import org.apache.storm.callback.WatcherCallBack;
import org.apache.storm.nimbus.ILeaderElector;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.ZookeeperAuthInfo;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class Zookeeper {

    private static Logger LOG = LoggerFactory.getLogger(Zookeeper.class);

    public static CuratorFramework mkClient(Map conf, List<String> servers, Object port, String root) {
        return mkClient(conf, servers, port, root, new DefaultWatcherCallBack());
    }

    public static CuratorFramework mkClient(Map conf, List<String> servers, Object port, Map authConf) {
        return mkClient(conf, servers, port, "", new DefaultWatcherCallBack(), authConf);
    }

    public static CuratorFramework mkClient(Map conf, List<String> servers, Object port, String root, Map authConf) {
        return mkClient(conf, servers, port, root, new DefaultWatcherCallBack(), authConf);
    }

    public static CuratorFramework mkClient(Map conf, List<String> servers, Object port, String root, final WatcherCallBack watcher, Map authConf) {
        CuratorFramework fk;
        if (authConf != null) {
            fk = Utils.newCurator(conf, servers, port, root, new ZookeeperAuthInfo(authConf));
        } else {
            fk = Utils.newCurator(conf, servers, port, root);
        }

        fk.getCuratorListenable().addListener(new CuratorListener() {
            @Override
            public void eventReceived(CuratorFramework _fk, CuratorEvent e) throws Exception {
                if (e.getType().equals(CuratorEventType.WATCHED)) {
                    WatchedEvent event = e.getWatchedEvent();

                    watcher.execute(event.getState(), event.getType(), event.getPath());
                }

            }
        });
        fk.start();
        return fk;
    }

    /**
     * connect ZK, register Watch/unhandle Watch
     *
     * @return
     */
    public static CuratorFramework mkClient(Map conf, List<String> servers, Object port, String root, final WatcherCallBack watcher) {

        return mkClient(conf, servers, port, root, watcher, null);
    }

    public static String createNode(CuratorFramework zk, String path, byte[] data, org.apache.zookeeper.CreateMode mode, List<ACL> acls)
            throws RuntimeException {

        String ret = null;
        try {
            String npath = Utils.normalizePath(path);
            ret = zk.create().withMode(mode).withACL(acls).forPath(npath, data);
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
        return ret;
    }

    public static String createNode(CuratorFramework zk, String path, byte[] data, List<ACL> acls) throws RuntimeException {
        return createNode(zk, path, data, org.apache.zookeeper.CreateMode.PERSISTENT, acls);
    }

    public static boolean existsNode(CuratorFramework zk, String path, boolean watch) throws RuntimeException {
        Stat stat = null;
        try {
            if (watch) {
                stat = zk.checkExists().watched().forPath(Utils.normalizePath(path));
            } else {
                stat = zk.checkExists().forPath(Utils.normalizePath(path));
            }
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }

        return stat != null;
    }

    public static void deleteNode(CuratorFramework zk, String path) throws RuntimeException {
        try {
            String npath = Utils.normalizePath(path);
            if (existsNode(zk, npath, false)) {
                zk.delete().deletingChildrenIfNeeded().forPath(Utils.normalizePath(path));
            }

        } catch (KeeperException.NoNodeException e) {
            LOG.info("exception", e);
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public static void mkdirs(CuratorFramework zk, String path, List<ACL> acls) throws RuntimeException {

        String npath = Utils.normalizePath(path);
        if (npath.equals("/")) {
            return;
        }
        if (existsNode(zk, npath, false)) {
            return;
        }
        byte[] byteArray = new byte[1];
        byteArray[0] = (byte) 7;
        createNode(zk, npath, byteArray, org.apache.zookeeper.CreateMode.PERSISTENT, acls);

    }

    public static void syncPath(CuratorFramework zk, String path) throws RuntimeException {
        try {
            zk.sync().forPath(Utils.normalizePath(path));
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public static void addListener(CuratorFramework zk, ConnectionStateListener listener) {
        zk.getConnectionStateListenable().addListener(listener);
    }

    public static byte[] getData(CuratorFramework zk, String path, boolean watch) throws RuntimeException {

        try {
            String npath = Utils.normalizePath(path);
            if (existsNode(zk, npath, watch)) {
                if (watch) {
                    return zk.getData().watched().forPath(npath);
                } else {
                    return zk.getData().forPath(npath);
                }
            }
        } catch (KeeperException e) {
            // this is fine b/c we still have a watch from the successful exists call
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }

        return null;
    }

    public static Integer getVersion(CuratorFramework zk, String path, boolean watch) throws Exception {
        String npath = Utils.normalizePath(path);
        Stat stat = null;
        if (existsNode(zk, npath, watch)) {
            if (watch) {
                stat = zk.checkExists().watched().forPath(Utils.normalizePath(path));
            } else {
                stat = zk.checkExists().forPath(Utils.normalizePath(path));
            }
            return Integer.valueOf(stat.getVersion());
        }

        return null;
    }

    public static List<String> getChildren(CuratorFramework zk, String path, boolean watch) throws RuntimeException {

        try {
            String npath = Utils.normalizePath(path);
            if (watch) {
                return zk.getChildren().watched().forPath(npath);
            } else {
                return zk.getChildren().forPath(npath);
            }
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    // Deletes the state inside the zookeeper for a key, for which the
    // contents of the key starts with nimbus host port information
    public static void deleteDodeBlobstore(CuratorFramework zk, String parentPath, String hostPortInfo) throws RuntimeException {
        String parentnPath = Utils.normalizePath(parentPath);
        List<String> childPathList = null;
        if (existsNode(zk, parentnPath, false)) {
            childPathList = getChildren(zk, parentnPath, false);
        }
        for (String child : childPathList) {
            if (child.startsWith(hostPortInfo)) {
                LOG.debug("deleteNode child " + child);
                deleteNode(zk, parentnPath + "/" + child);
            }
        }
    }

    public static Stat setData(CuratorFramework zk, String path, byte[] data) throws RuntimeException {

        try {
            String npath = Utils.normalizePath(path);
            return zk.setData().forPath(npath, data);
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public static boolean exists(CuratorFramework zk, String path, boolean watch) throws RuntimeException {
        return existsNode(zk, path, watch);
    }

    public static NIOServerCnxnFactory mkInprocessZookeeper(String localdir, Integer port) throws IOException, InterruptedException {
        LOG.info("Starting inprocess zookeeper at port " + port + " and dir " + localdir);
        File localfile = new File(localdir);
        ZooKeeperServer zk = new ZooKeeperServer(localfile, localfile, 2000);

        NIOServerCnxnFactory factory = null;
        int report = 2000;
        int limitPort = 65535;
        if (port != null) {
            report = port;
            limitPort = port;
        }
        while (true) {
            try {
                factory = new NIOServerCnxnFactory();
                factory.configure(new InetSocketAddress(port), 0);
                break;
            } catch (BindException e) {
                report++;
                if (report > limitPort) {
                    throw new RuntimeException("No port is available to launch an inprocess zookeeper");
                }
            }
        }
        factory.startup(zk);
        return factory;
    }

    public static void shutdownInprocessZookeeper(Factory handle) {
        handle.shutdown();
    }

    public static NimbusInfo toNimbusInfo(Participant participant) {
        String id = participant.getId();
        if (StringUtils.isBlank(id)) {
            throw new RuntimeException("No nimbus leader participant host found, have you started your nimbus hosts?");
        }
        NimbusInfo nimbusInfo = NimbusInfo.parse(id);
        nimbusInfo.setLeader(participant.isLeader());
        return nimbusInfo;
    }

    // Leader latch listener that will be invoked when we either gain or lose leadership
    public static LeaderLatchListener leaderLatchListenerImpl(Map conf, CuratorFramework zk, LeaderLatch leaderLatch) throws Exception {
        final String hostName = InetAddress.getLocalHost().getCanonicalHostName();
        return new LeaderLatchListener() {
            @Override
            public void isLeader() {
                LOG.info(hostName + " gained leadership");
            }

            @Override
            public void notLeader() {
                LOG.info(hostName + " lost leadership.");
            }
        };
    }

    public static ILeaderElector zkLeaderElector(Map conf) throws Exception {
        List<String> servers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);
        CuratorFramework zk = mkClient(conf, servers, port, "", conf);
        String leaderLockPath = conf.get(Config.STORM_ZOOKEEPER_ROOT) + "/leader-lock";
        String id = NimbusInfo.fromConf(conf).toHostPortString();
        AtomicReference<LeaderLatch> leaderLatchAtomicReference = new AtomicReference<>(new LeaderLatch(zk, leaderLockPath, id));
        AtomicReference<LeaderLatchListener> leaderLatchListenerAtomicReference =
                new AtomicReference<>(leaderLatchListenerImpl(conf, zk, leaderLatchAtomicReference.get()));
        return new LeaderElectorImp(conf, servers, zk, leaderLockPath, id, leaderLatchAtomicReference, leaderLatchListenerAtomicReference);
    }

    //To do modify @return once don't need persistentArrayMap
    public static PersistentArrayMap getDataWithVersion(CuratorFramework zk, String path, boolean watch) {
        PersistentArrayMap map = null;
        try {
            byte[] bytes = null;
            Stat stats = new Stat();
            String npath = Utils.normalizePath(path);
            if (existsNode(zk, npath, watch)) {
                if (watch) {
                    bytes = zk.getData().storingStatIn(stats).watched().forPath(npath);
                } else {
                    bytes = zk.getData().storingStatIn(stats).forPath(npath);

                }
                if (bytes != null) {
                    int version = stats.getVersion();
                    map = new PersistentArrayMap(new Object[] { RT.keyword(null, "data"), bytes, RT.keyword(null, "version"), version });
                }
            }
        } catch (KeeperException.NoNodeException e) {
            // this is fine b/c we still have a watch from the successful exists call
        } catch (Exception e){
            Utils.wrapInRuntime(e);
        }
        return map;
    }

}