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
package storm.kafka;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZkState {
    public static final Logger LOG = LoggerFactory.getLogger(ZkState.class);
    CuratorFramework _curator;

    private CuratorFramework newCurator(Map stateConf) throws Exception {
        Integer port = (Integer) stateConf.get(Config.TRANSACTIONAL_ZOOKEEPER_PORT);
        String serverPorts = "";
        for (String server : (List<String>) stateConf.get(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS)) {
            serverPorts = serverPorts + server + ":" + port + ",";
        }
        return CuratorFrameworkFactory.newClient(serverPorts,
                Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)),
                Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)),
                new RetryNTimes(Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)),
                        Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
    }

    public CuratorFramework getCurator() {
        assert _curator != null;
        return _curator;
    }

    public ZkState(Map stateConf) {
        stateConf = new HashMap(stateConf);

        try {
            _curator = newCurator(stateConf);
            _curator.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void writeJSON(String path, Map<Object, Object> data) {
        LOG.debug("Writing " + path + " the data " + data.toString());
        writeBytes(path, JSONValue.toJSONString(data).getBytes(Charset.forName("UTF-8")));
    }

    public void writeBytes(String path, byte[] bytes) {
        try {
            if (_curator.checkExists().forPath(path) == null) {
                _curator.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path, bytes);
            } else {
                _curator.setData().forPath(path, bytes);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<Object, Object> readJSON(String path) {
        try {
            byte[] b = readBytes(path);
            if (b == null) {
                return null;
            }
            return (Map<Object, Object>) JSONValue.parse(new String(b, "UTF-8"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] readBytes(String path) {
        try {
            if (_curator.checkExists().forPath(path) != null) {
                return _curator.getData().forPath(path);
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        _curator.close();
        _curator = null;
    }
}
