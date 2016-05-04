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
package org.apache.storm.callback;

import org.apache.storm.zookeeper.ZkEventTypes;
import org.apache.storm.zookeeper.ZkKeeperStates;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultWatcherCallBack implements WatcherCallBack {

    private static Logger LOG = LoggerFactory.getLogger(DefaultWatcherCallBack.class);

    @Override
    public void execute(Watcher.Event.KeeperState state, Watcher.Event.EventType type, String path) {
        LOG.debug("Zookeeper state update:  {}, {}, {}", ZkKeeperStates.getStateName(state), ZkEventTypes.getTypeName(type), path);
    }

}
