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
package org.apache.storm.cluster;

import clojure.lang.APersistentMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.*;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.storm.Config;
import org.apache.storm.callback.Callback;
import org.apache.storm.callback.WatcherCallBack;
import org.apache.storm.utils.Utils;
import org.apache.storm.zookeeper.Zookeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class DistributedClusterState implements ClusterState {

    private static Logger LOG = LoggerFactory.getLogger(DistributedClusterState.class);

    private ConcurrentHashMap<String, Callback> callbacks = new ConcurrentHashMap<String, Callback>();
    private CuratorFramework zkWriter;
    private CuratorFramework zkReader;
    private AtomicBoolean active;

    private boolean isNimbus;
    private Map authConf;
    private Map<Object, Object> conf;

    public DistributedClusterState(Map<Object, Object> conf, Map authConf, List<ACL> acls, ClusterStateContext context) throws Exception {
        this.conf = conf;
        this.authConf = authConf;
        if (context.getDaemonType().equals(DaemonType.NIMBUS)) this.isNimbus = true;

        // just mkdir STORM_ZOOKEEPER_ROOT dir
        CuratorFramework zkTemp = mkZk();
        String rootPath = String.valueOf(conf.get(Config.STORM_ZOOKEEPER_ROOT));
        Zookeeper.mkdirs(zkTemp, rootPath, acls);
        zkTemp.close();

        active = new AtomicBoolean(true);
        zkWriter = mkZk(new WatcherCallBack() {
            @Override
            public void execute(Watcher.Event.KeeperState state, Watcher.Event.EventType type, String path) {
                if (active.get()) {
                    if (!(state.equals(Watcher.Event.KeeperState.SyncConnected))) {
                        LOG.warn("Received event {} : {}: {} with disconnected Zookeeper.", state, type, path);
                    } else {
                        LOG.info("Received event {} : {} : {}", state, type, path);
                    }

                    if (!type.equals(Watcher.Event.EventType.None)) {
                        for (Map.Entry<String, Callback> e : callbacks.entrySet()) {
                            Callback fn = e.getValue();
                            fn.execute(type, path);
                        }
                    }
                }
            }
        });
        if (isNimbus) {
            zkReader = mkZk(new WatcherCallBack() {
                @Override
                public void execute(Watcher.Event.KeeperState state, Watcher.Event.EventType type, String path) {
                    if (active.get()) {
                        if (!(state.equals(Watcher.Event.KeeperState.SyncConnected))) {
                            LOG.warn("Received event {} : {}: {} with disconnected Zookeeper.", state, type, path);
                        } else {
                            LOG.info("Received event {} : {} : {}", state, type, path);
                        }

                        if (!type.equals(Watcher.Event.EventType.None)) {
                            for (Map.Entry<String, Callback> e : callbacks.entrySet()) {
                                Callback fn = e.getValue();
                                fn.execute(type, path);
                            }
                        }
                    }
                }
            });
        } else {
            zkReader = zkWriter;
        }

    }

    @SuppressWarnings("unchecked")
    private CuratorFramework mkZk() throws IOException {
        return Zookeeper.mkClient(conf, (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS), conf.get(Config.STORM_ZOOKEEPER_PORT), "", authConf);
    }

    @SuppressWarnings("unchecked")
    private CuratorFramework mkZk(WatcherCallBack watcher) throws NumberFormatException, IOException {
        return Zookeeper.mkClient(conf, (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS), conf.get(Config.STORM_ZOOKEEPER_PORT),
                String.valueOf(conf.get(Config.STORM_ZOOKEEPER_ROOT)), watcher, authConf);
    }

    @Override
    public void delete_node_blobstore(String path, String nimbusHostPortInfo) {

    }

    @Override
    public String register( Callback callback) {
        String id = UUID.randomUUID().toString();
        this.callbacks.put(id,callback);
        return id;
    }

    @Override
    public void unregister(String id) {
        this.callbacks.remove(id);
    }

    @Override
    public String create_sequential(String path, byte[] data, List<ACL> acls) {
        return Zookeeper.createNode(zkWriter, path, data, CreateMode.EPHEMERAL_SEQUENTIAL, acls);
    }

    @Override
    public void mkdirs(String path, List<ACL> acls) {
        Zookeeper.mkdirs(zkWriter, path, acls);
    }

    @Override
    public void delete_node(String path) {
        Zookeeper.deleteNode(zkWriter, path);
    }

    @Override
    public void set_ephemeral_node(String path, byte[] data, List<ACL> acls) {
        Zookeeper.mkdirs(zkWriter, parentPath(path), acls);
        if (Zookeeper.exists(zkWriter, path, false)) {
            try {
                Zookeeper.setData(zkWriter, path, data);
            } catch (RuntimeException e) {
                if (Utils.exceptionCauseIsInstanceOf(KeeperException.NodeExistsException.class, e)) {
                    Zookeeper.createNode(zkWriter, path, data, CreateMode.EPHEMERAL, acls);
                } else {
                    throw e;
                }
            }

        } else {
            Zookeeper.createNode(zkWriter, path, data, CreateMode.EPHEMERAL, acls);
        }
    }

    @Override
    public Integer get_version(String path, boolean watch) throws Exception {
        Integer ret = Zookeeper.getVersion(zkReader, path, watch);
        return ret;
    }

    @Override
    public boolean node_exists(String path, boolean watch) {
        return Zookeeper.existsNode(zkWriter, path, watch);
    }

    @Override
    public List<String> get_children(String path, boolean watch) {
        return Zookeeper.getChildren(zkReader, path, watch);
    }

    @Override
    public void close() {
        this.active.set(false);
        zkWriter.close();
        if (isNimbus) {
            zkReader.close();
        }
    }

    @Override
    public void set_data(String path, byte[] data, List<ACL> acls) {
        if (Zookeeper.exists(zkWriter, path, false)) {
            Zookeeper.setData(zkWriter, path, data);
        } else {
            Zookeeper.mkdirs(zkWriter, parentPath(path), acls);
            Zookeeper.createNode(zkWriter, path, data, CreateMode.PERSISTENT, acls);
        }
    }

    @Override
    public byte[] get_data(String path, boolean watch) {
        byte[] ret = null;

        ret = Zookeeper.getData(zkReader, path, watch);

        return ret;
    }

    @Override
    public APersistentMap get_data_with_version(String path, boolean watch) {
        return Zookeeper.getDataWithVersion(zkReader, path, watch);
    }

    @Override
    public void set_worker_hb(String path, byte[] data, List<ACL> acls) {
        set_data(path, data, acls);
    }

    @Override
    public byte[] get_worker_hb(String path, boolean watch) {
        return Zookeeper.getData(zkReader, path, watch);
    }

    @Override
    public List<String> get_worker_hb_children(String path, boolean watch) {
        return get_children(path, watch);
    }

    @Override
    public void delete_worker_hb(String path) {
        delete_node(path);
    }

    @Override
    public void add_listener(final ConnectionStateListener listener) {
        Zookeeper.addListener(zkReader, new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                listener.stateChanged(curatorFramework, connectionState);
            }
        });
    }

    @Override
    public void sync_path(String path) {
        Zookeeper.syncPath(zkWriter, path);
    }

    // To be remove when finished port Util.clj
    public static String parentPath(String path) {
        List<String> toks = Zookeeper.tokenizePath(path);
        int size = toks.size();
        if (size > 0) {
            toks.remove(size - 1);
        }
        return Zookeeper.toksToPath(toks);
    }
}
