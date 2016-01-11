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
package org.apache.storm.transactional.state;

import org.apache.storm.Config;
import org.apache.storm.serialization.KryoValuesDeserializer;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.ZookeeperAuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ProtectACLCreateModePathAndBytesable;
import org.apache.curator.framework.api.PathAndBytesable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

public class TransactionalState {
    CuratorFramework _curator;
    KryoValuesSerializer _ser;
    KryoValuesDeserializer _des;
    List<ACL> _zkAcls = null;
    
    public static TransactionalState newUserState(Map conf, String id, Map componentConf) {
        return new TransactionalState(conf, id, componentConf, "user");
    }
    
    public static TransactionalState newCoordinatorState(Map conf, String id, Map componentConf) {
        return new TransactionalState(conf, id, componentConf, "coordinator");        
    }
    
    protected TransactionalState(Map conf, String id, Map componentConf, String subroot) {
        try {
            conf = new HashMap(conf);
            // ensure that the serialization registrations are consistent with the declarations in this spout
            if(componentConf!=null) {
                conf.put(Config.TOPOLOGY_KRYO_REGISTER,
                         componentConf
                              .get(Config.TOPOLOGY_KRYO_REGISTER));
            }
            String transactionalRoot = (String)conf.get(Config.TRANSACTIONAL_ZOOKEEPER_ROOT);
            String rootDir = transactionalRoot + "/" + id + "/" + subroot;
            List<String> servers = (List<String>) getWithBackup(conf, Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, Config.STORM_ZOOKEEPER_SERVERS);
            Object port = getWithBackup(conf, Config.TRANSACTIONAL_ZOOKEEPER_PORT, Config.STORM_ZOOKEEPER_PORT);
            ZookeeperAuthInfo auth = new ZookeeperAuthInfo(conf);
            CuratorFramework initter = Utils.newCuratorStarted(conf, servers, port, auth);
            _zkAcls = Utils.getWorkerACL(conf);
            try {
                TransactionalState.createNode(initter, transactionalRoot, null, null, null);
            } catch (KeeperException.NodeExistsException e) {
            }
            try {
                TransactionalState.createNode(initter, rootDir, null, _zkAcls, null);
            } catch (KeeperException.NodeExistsException e) {
            }
            initter.close();
                                    
            _curator = Utils.newCuratorStarted(conf, servers, port, rootDir, auth);
            _ser = new KryoValuesSerializer(conf);
            _des = new KryoValuesDeserializer(conf);
        } catch (Exception e) {
           throw new RuntimeException(e);
        }
    }

    protected static String forPath(PathAndBytesable<String> builder, 
            String path, byte[] data) throws Exception {
        return (data == null) 
            ? builder.forPath(path) 
            : builder.forPath(path, data);
    }

    protected static void createNode(CuratorFramework curator, String path,
            byte[] data, List<ACL> acls, CreateMode mode) throws Exception {
        ProtectACLCreateModePathAndBytesable<String> builder =
            curator.create().creatingParentsIfNeeded();
    
        if (acls == null) {
            if (mode == null ) {
                TransactionalState.forPath(builder, path, data);
            } else {
                TransactionalState.forPath(builder.withMode(mode), path, data);
            }
            return;
        }

        TransactionalState.forPath(builder.withACL(acls), path, data);
    }

    public void setData(String path, Object obj) {
        path = "/" + path;
        byte[] ser = _ser.serializeObject(obj);
        try {
            if(_curator.checkExists().forPath(path)!=null) {
                _curator.setData().forPath(path, ser);
            } else {
                TransactionalState.createNode(_curator, path, ser, _zkAcls,
                        CreateMode.PERSISTENT);
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }        
    }
    
    public void delete(String path) {
        path = "/" + path;
        try {
            _curator.delete().forPath(path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public List<String> list(String path) {
        path = "/" + path;
        try {
            if(_curator.checkExists().forPath(path)==null) {
                return new ArrayList<String>();
            } else {
                return _curator.getChildren().forPath(path);
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }   
    }
    
    public void mkdir(String path) {
        setData(path, 7);
    }
    
    public Object getData(String path) {
        path = "/" + path;
        try {
            if(_curator.checkExists().forPath(path)!=null) {
                return _des.deserializeObject(_curator.getData().forPath(path));
            } else {
                return null;
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public void close() {
        _curator.close();
    }
    
    private Object getWithBackup(Map amap, Object primary, Object backup) {
        Object ret = amap.get(primary);
        if(ret==null) return amap.get(backup);
        return ret;
    }
}
