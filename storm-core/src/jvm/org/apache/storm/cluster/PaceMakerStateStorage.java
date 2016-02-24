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

import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.storm.callback.ZKStateChangedCallback;
import org.apache.storm.generated.*;
import org.apache.storm.pacemaker.PacemakerClient;
import org.apache.storm.utils.Utils;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class PaceMakerStateStorage implements IStateStorage {

    private static Logger LOG = LoggerFactory.getLogger(PaceMakerStateStorage.class);

    private PacemakerClient pacemakerClient;
    private IStateStorage stateStorage;
    private static final int maxRetries = 10;

    public PaceMakerStateStorage(PacemakerClient pacemakerClient, IStateStorage stateStorage) throws Exception {
        this.pacemakerClient = pacemakerClient;
        this.stateStorage = stateStorage;
    }

    @Override
    public String register(ZKStateChangedCallback callback) {
        return stateStorage.register(callback);
    }

    @Override
    public void unregister(String id) {
        stateStorage.unregister(id);
    }

    @Override
    public String create_sequential(String path, byte[] data, List<ACL> acls) {
        return stateStorage.create_sequential(path, data, acls);
    }

    @Override
    public void mkdirs(String path, List<ACL> acls) {
        stateStorage.mkdirs(path, acls);
    }

    @Override
    public void delete_node(String path) {
        stateStorage.delete_node(path);
    }

    @Override
    public void set_ephemeral_node(String path, byte[] data, List<ACL> acls) {
        stateStorage.set_ephemeral_node(path, data, acls);
    }

    @Override
    public Integer get_version(String path, boolean watch) throws Exception {
        return stateStorage.get_version(path, watch);
    }

    @Override
    public boolean node_exists(String path, boolean watch) {
        return stateStorage.node_exists(path, watch);
    }

    @Override
    public List<String> get_children(String path, boolean watch) {
        return stateStorage.get_children(path, watch);
    }

    @Override
    public void close() {
        stateStorage.close();
        pacemakerClient.close();
    }

    @Override
    public void set_data(String path, byte[] data, List<ACL> acls) {
        stateStorage.set_data(path, data, acls);
    }

    @Override
    public byte[] get_data(String path, boolean watch) {
        return stateStorage.get_data(path, watch);
    }

    @Override
    public Map get_data_with_version(String path, boolean watch) {
        return stateStorage.get_data_with_version(path, watch);
    }

    @Override
    public void set_worker_hb(String path, byte[] data, List<ACL> acls) {
        int retry = maxRetries;
        while (true) {
            try {
                HBPulse hbPulse = new HBPulse();
                hbPulse.set_id(path);
                hbPulse.set_details(data);
                HBMessage message = new HBMessage(HBServerMessageType.SEND_PULSE, HBMessageData.pulse(hbPulse));
                HBMessage response = pacemakerClient.send(message);
                if (response.get_type() != HBServerMessageType.SEND_PULSE_RESPONSE) {
                    throw new HBExecutionException("Invalid Response Type");
                }
                LOG.debug("Successful set_worker_hb");
                break;
            } catch (Exception e) {
                if (retry <= 0) {
                    throw Utils.wrapInRuntime(e);
                }
                retry--;
                LOG.error("{} Failed to set_worker_hb. Will make {} more attempts.", e.getMessage(), retry);
            }
        }
    }

    @Override
    public byte[] get_worker_hb(String path, boolean watch) {
        int retry = maxRetries;
        while (true) {
            try {
                HBMessage message = new HBMessage(HBServerMessageType.GET_PULSE, HBMessageData.path(path));
                HBMessage response = pacemakerClient.send(message);
                if (response.get_type() != HBServerMessageType.GET_PULSE_RESPONSE) {
                    throw new HBExecutionException("Invalid Response Type");
                }
                LOG.debug("Successful get_worker_hb");
                return response.get_data().get_pulse().get_details();
            } catch (Exception e) {
                if (retry <= 0) {
                    throw Utils.wrapInRuntime(e);
                }
                retry--;
                LOG.error("{} Failed to get_worker_hb. Will make {} more attempts.", e.getMessage(), retry);
            }
        }
    }

    @Override
    public List<String> get_worker_hb_children(String path, boolean watch) {
        int retry = maxRetries;
        while (true) {
            try {
                HBMessage message = new HBMessage(HBServerMessageType.GET_ALL_NODES_FOR_PATH, HBMessageData.path(path));
                HBMessage response = pacemakerClient.send(message);
                if (response.get_type() != HBServerMessageType.GET_ALL_NODES_FOR_PATH_RESPONSE) {
                    throw new HBExecutionException("Invalid Response Type");
                }
                LOG.debug("Successful get_worker_hb");
                return response.get_data().get_nodes().get_pulseIds();
            } catch (Exception e) {
                if (retry <= 0) {
                    throw Utils.wrapInRuntime(e);
                }
                retry--;
                LOG.error("{} Failed to get_worker_hb_children. Will make {} more attempts.", e.getMessage(), retry);
            }
        }
    }

    @Override
    public void delete_worker_hb(String path) {
        int retry = maxRetries;
        while (true) {
            try {
                HBMessage message = new HBMessage(HBServerMessageType.DELETE_PATH, HBMessageData.path(path));
                HBMessage response = pacemakerClient.send(message);
                if (response.get_type() != HBServerMessageType.DELETE_PATH_RESPONSE) {
                    throw new HBExecutionException("Invalid Response Type");
                }
                LOG.debug("Successful get_worker_hb");
                break;
            } catch (Exception e) {
                if (retry <= 0) {
                    throw Utils.wrapInRuntime(e);
                }
                retry--;
                LOG.error("{} Failed to delete_worker_hb. Will make {} more attempts.", e.getMessage(), retry);
            }
        }
    }

    @Override
    public void add_listener(ConnectionStateListener listener) {
        stateStorage.add_listener(listener);
    }

    @Override
    public void sync_path(String path) {
        stateStorage.sync_path(path);
    }

    @Override
    public void delete_node_blobstore(String path, String nimbusHostPortInfo) {
        stateStorage.delete_node_blobstore(path, nimbusHostPortInfo);
    }
}
