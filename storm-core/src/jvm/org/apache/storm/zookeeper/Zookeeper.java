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

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.storm.Config;
import org.apache.storm.callback.DefaultWatcherCallBack;
import org.apache.storm.callback.WatcherCallBack;
import org.apache.storm.cluster.IStateStorage;
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
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


public class Zookeeper {
    private static Logger LOG = LoggerFactory.getLogger(Zookeeper.class);

    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static final Zookeeper INSTANCE = new Zookeeper();
    private static Zookeeper _instance = INSTANCE;

    /**
     * Provide an instance of this class for delegates to use.  To mock out
     * delegated methods, provide an instance of a subclass that overrides the
     * implementation of the delegated method.
     *
     * @param u a Zookeeper instance
     */
    public static void setInstance(Zookeeper u) {
        _instance = u;
    }

    /**
     * Resets the singleton instance to the default. This is helpful to reset
     * the class to its original functionality when mocking is no longer
     * desired.
     */
    public static void resetInstance() {
        _instance = INSTANCE;
    }

    public  CuratorFramework mkClientImpl(Map conf, List<String> servers, Object port, String root) {
        return mkClientImpl(conf, servers, port, root, new DefaultWatcherCallBack());
    }

    public  CuratorFramework mkClientImpl(Map conf, List<String> servers, Object port, Map authConf) {
        return mkClientImpl(conf, servers, port, "", new DefaultWatcherCallBack(), authConf);
    }

    public  CuratorFramework mkClientImpl(Map conf, List<String> servers, Object port, String root, Map authConf) {
        return mkClientImpl(conf, servers, port, root, new DefaultWatcherCallBack(), authConf);
    }

    public static CuratorFramework mkClient(Map conf, List<String> servers, Object port, String root, final WatcherCallBack watcher, Map authConf) {
        return _instance.mkClientImpl(conf, servers, port, root, watcher, authConf);
    }

    public  CuratorFramework mkClientImpl(Map conf, List<String> servers, Object port, String root, final WatcherCallBack watcher, Map authConf) {
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
    public  CuratorFramework mkClientImpl(Map conf, List<String> servers, Object port, String root, final WatcherCallBack watcher) {
        return mkClientImpl(conf, servers, port, root, watcher, null);
    }

    public static String createNode(CuratorFramework zk, String path, byte[] data, org.apache.zookeeper.CreateMode mode, List<ACL> acls) {
        String ret = null;
        try {
            String npath = normalizePath(path);
            ret = zk.create().creatingParentsIfNeeded().withMode(mode).withACL(acls).forPath(npath, data);
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
        return ret;
    }

    public static String createNode(CuratorFramework zk, String path, byte[] data, List<ACL> acls){
        return createNode(zk, path, data, org.apache.zookeeper.CreateMode.PERSISTENT, acls);
    }

    public static boolean existsNode(CuratorFramework zk, String path, boolean watch){
        Stat stat = null;
        try {
            if (watch) {
                stat = zk.checkExists().watched().forPath(normalizePath(path));
            } else {
                stat = zk.checkExists().forPath(normalizePath(path));
            }
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
        return stat != null;
    }

    public static void deleteNode(CuratorFramework zk, String path){
        try {
            String npath = normalizePath(path);
            if (existsNode(zk, npath, false)) {
                zk.delete().deletingChildrenIfNeeded().forPath(normalizePath(path));
            }
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.NodeExistsException.class, e)) {
                // do nothing
                LOG.info("delete {} failed.", path, e);
            } else {
                throw Utils.wrapInRuntime(e);
            }
        }
    }

    public static void mkdirs(CuratorFramework zk, String path, List<ACL> acls) {
        _instance.mkdirsImpl(zk, path, acls);
    }

    public void mkdirsImpl(CuratorFramework zk, String path, List<ACL> acls) {
        String npath = normalizePath(path);
        if (npath.equals("/")) {
            return;
        }
        if (existsNode(zk, npath, false)) {
            return;
        }
        byte[] byteArray = new byte[1];
        byteArray[0] = (byte) 7;
        try {
            createNode(zk, npath, byteArray, org.apache.zookeeper.CreateMode.PERSISTENT, acls);
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.NodeExistsException.class, e)) {
                // this can happen when multiple clients doing mkdir at same time
            }
        }
    }

    public static void syncPath(CuratorFramework zk, String path){
        try {
            zk.sync().forPath(normalizePath(path));
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public static void addListener(CuratorFramework zk, ConnectionStateListener listener) {
        zk.getConnectionStateListenable().addListener(listener);
    }

    public static byte[] getData(CuratorFramework zk, String path, boolean watch){
        try {
            String npath = normalizePath(path);
            if (existsNode(zk, npath, watch)) {
                if (watch) {
                    return zk.getData().watched().forPath(npath);
                } else {
                    return zk.getData().forPath(npath);
                }
            }
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.NoNodeException.class, e)) {
                // this is fine b/c we still have a watch from the successful exists call
            } else {
                throw Utils.wrapInRuntime(e);
            }
        }
        return null;
    }

    public static Integer getVersion(CuratorFramework zk, String path, boolean watch) throws Exception {
        String npath = normalizePath(path);
        Stat stat = null;
        if (existsNode(zk, npath, watch)) {
            if (watch) {
                stat = zk.checkExists().watched().forPath(npath);
            } else {
                stat = zk.checkExists().forPath(npath);
            }
        }
        return stat == null ? null : Integer.valueOf(stat.getVersion());
    }

    public static List<String> getChildren(CuratorFramework zk, String path, boolean watch) {
        try {
            String npath = normalizePath(path);
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
    public static void deleteNodeBlobstore(CuratorFramework zk, String parentPath, String hostPortInfo){
        String normalizedPatentPath = normalizePath(parentPath);
        List<String> childPathList = null;
        if (existsNode(zk, normalizedPatentPath, false)) {
            childPathList = getChildren(zk, normalizedPatentPath, false);
            for (String child : childPathList) {
                if (child.startsWith(hostPortInfo)) {
                    LOG.debug("deleteNode child {}", child);
                    deleteNode(zk, normalizedPatentPath + "/" + child);
                }
            }
        }
    }

    public static Stat setData(CuratorFramework zk, String path, byte[] data){
        try {
            String npath = normalizePath(path);
            return zk.setData().forPath(npath, data);
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public static boolean exists(CuratorFramework zk, String path, boolean watch){
        return existsNode(zk, path, watch);
    }

    public static List mkInprocessZookeeper(String localdir, Integer port) throws Exception {
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
                factory.configure(new InetSocketAddress(report), 0);
                break;
            } catch (BindException e) {
                report++;
                if (report > limitPort) {
                    throw new RuntimeException("No port is available to launch an inprocess zookeeper");
                }
            }
        }
        LOG.info("Starting inprocess zookeeper at port {} and dir {}", report, localdir);
        factory.startup(zk);
        return Arrays.asList((Object) new Long(report), (Object) factory);
    }

    public static void shutdownInprocessZookeeper(NIOServerCnxnFactory handle) {
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
    public static LeaderLatchListener leaderLatchListenerImpl(Map conf, CuratorFramework zk, LeaderLatch leaderLatch) throws UnknownHostException {
        final String hostName = InetAddress.getLocalHost().getCanonicalHostName();
        return new LeaderLatchListener() {
            @Override
            public void isLeader() {
                LOG.info("{} gained leadership", hostName);
            }

            @Override
            public void notLeader() {
                LOG.info("{} lost leadership.", hostName);
            }
        };
    }

    public static ILeaderElector zkLeaderElector(Map conf) throws UnknownHostException {
        return _instance.zkLeaderElectorImpl(conf);
    }

    protected ILeaderElector zkLeaderElectorImpl(Map conf) throws UnknownHostException {
        List<String> servers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);
        CuratorFramework zk = mkClientImpl(conf, servers, port, "", conf);
        String leaderLockPath = conf.get(Config.STORM_ZOOKEEPER_ROOT) + "/leader-lock";
        String id = NimbusInfo.fromConf(conf).toHostPortString();
        AtomicReference<LeaderLatch> leaderLatchAtomicReference = new AtomicReference<>(new LeaderLatch(zk, leaderLockPath, id));
        AtomicReference<LeaderLatchListener> leaderLatchListenerAtomicReference =
                new AtomicReference<>(leaderLatchListenerImpl(conf, zk, leaderLatchAtomicReference.get()));
        return new LeaderElectorImp(conf, servers, zk, leaderLockPath, id, leaderLatchAtomicReference, leaderLatchListenerAtomicReference);
    }

    public static Map getDataWithVersion(CuratorFramework zk, String path, boolean watch) {
        Map map = new HashMap();
        try {
            byte[] bytes = null;
            Stat stats = new Stat();
            String npath = normalizePath(path);
            if (existsNode(zk, npath, watch)) {
                if (watch) {
                    bytes = zk.getData().storingStatIn(stats).watched().forPath(npath);
                } else {
                    bytes = zk.getData().storingStatIn(stats).forPath(npath);
                }
                if (bytes != null) {
                    int version = stats.getVersion();
                    map.put(IStateStorage.DATA, bytes);
                    map.put(IStateStorage.VERSION, version);
                }
            }
        } catch (Exception e) {
            if (Utils.exceptionCauseIsInstanceOf(KeeperException.NoNodeException.class, e)) {
                // this is fine b/c we still have a watch from the successful exists call
            } else {
                Utils.wrapInRuntime(e);
            }
        }
        return map;
    }

    public static List<String> tokenizePath(String path) {
        String[] toks = path.split("/");
        java.util.ArrayList<String> rtn = new ArrayList<String>();
        for (String str : toks) {
            if (!str.isEmpty()) {
                rtn.add(str);
            }
        }
        return rtn;
    }

    public static String parentPath(String path) {
        List<String> toks = Zookeeper.tokenizePath(path);
        int size = toks.size();
        if (size > 0) {
            toks.remove(size - 1);
        }
        return Zookeeper.toksToPath(toks);
    }

    public static String toksToPath(List<String> toks) {
        StringBuffer buff = new StringBuffer();
        buff.append("/");
        int size = toks.size();
        for (int i = 0; i < size; i++) {
            buff.append(toks.get(i));
            if (i < (size - 1)) {
                buff.append("/");
            }
        }
        return buff.toString();
    }

    public static String normalizePath(String path) {
        String rtn = toksToPath(tokenizePath(path));
        return rtn;
    }
}
