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

package org.apache.storm.utils;

import org.apache.storm.cluster.ConnectionState;

import java.util.HashMap;
import java.util.Map;

public class StormConnectionStateConverter {

    private static final Map<org.apache.curator.framework.state.ConnectionState, ConnectionState> mapCuratorToStorm = new HashMap<>();
    static {
        mapCuratorToStorm.put(org.apache.curator.framework.state.ConnectionState.CONNECTED, ConnectionState.CONNECTED);
        mapCuratorToStorm.put(org.apache.curator.framework.state.ConnectionState.LOST, ConnectionState.LOST);
        mapCuratorToStorm.put(org.apache.curator.framework.state.ConnectionState.RECONNECTED, ConnectionState.RECONNECTED);
        mapCuratorToStorm.put(org.apache.curator.framework.state.ConnectionState.READ_ONLY, ConnectionState.LOST);
        mapCuratorToStorm.put(org.apache.curator.framework.state.ConnectionState.SUSPENDED, ConnectionState.LOST);
    }

    public static ConnectionState convert(org.apache.curator.framework.state.ConnectionState state) {
        ConnectionState stormState = mapCuratorToStorm.get(state);
        if (stormState != null) {
            return stormState;
        }
        throw new IllegalStateException("Unknown ConnectionState from Curator: " + state);
    }
}
