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

package org.apache.storm.trident.topology.state;

import java.util.List;
import java.util.Map;

import org.apache.storm.utils.ZookeeperAuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

/**
 * Facilitates testing of non-public methods in the parent class.
 */
public class TestTransactionalState extends TransactionalState {

    /**
     * Matching constructor in absence of a default constructor in the parent
     * class.
     */
    protected TestTransactionalState(Map conf, String id, String subroot) {
        super(conf, id, subroot);
    }

    public static void createNode(CuratorFramework curator, 
            String rootDir, byte[] data, List<ACL> acls, CreateMode mode)
            throws Exception {
       TransactionalState.createNode(curator, rootDir, data, acls, mode);
    }
}
