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

import org.apache.storm.pacemaker.PacemakerClient;
import org.apache.storm.utils.Utils;
import org.apache.zookeeper.data.ACL;

import java.util.List;
import java.util.Map;

public class PaceMakerStateStorageFactory implements StateStorageFactory {

    private static final PaceMakerStateStorageFactory INSTANCE = new PaceMakerStateStorageFactory();
    private static PaceMakerStateStorageFactory _instance = INSTANCE;

    public static void setInstance(PaceMakerStateStorageFactory u) {
        _instance = u;
    }

    public static void resetInstance() {
        _instance = INSTANCE;
    }

    @Override
    public IStateStorage mkStore(Map config, Map auth_conf, List<ACL> acls, ClusterStateContext context) {
        try {
            return new PaceMakerStateStorage(initMakeClient(config), initZKstate(config, auth_conf, acls, context));
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public static IStateStorage initZKstate(Map config, Map auth_conf, List<ACL> acls, ClusterStateContext context) throws Exception {
        return _instance.initZKstateImpl(config, auth_conf, acls, context);
    }

    public static PacemakerClient initMakeClient(Map config) {
        return _instance.initMakeClientImpl(config);
    }

    public IStateStorage initZKstateImpl(Map config, Map auth_conf, List<ACL> acls, ClusterStateContext context) throws Exception {
        ZKStateStorageFactory zkStateStorageFactory = new ZKStateStorageFactory();
        return zkStateStorageFactory.mkStore(config, auth_conf, acls, context);
    }

    public PacemakerClient initMakeClientImpl(Map config) {
        return new PacemakerClient(config);
    }
}
