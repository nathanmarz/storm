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
package org.apache.storm.state;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A factory for creating {@link State} instances
 */
public class StateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(StateFactory.class);

    private static final String DEFAULT_PROVIDER = "org.apache.storm.state.InMemoryKeyValueStateProvider";

    /**
     * Returns a new state instance using the {@link Config#TOPOLOGY_STATE_PROVIDER} or a
     * {@link InMemoryKeyValueState} if no provider is configured.
     *
     * @param namespace the state namespace
     * @param stormConf the storm conf
     * @param context   the topology context
     * @return the state instance
     */
    public static State getState(String namespace, Map stormConf, TopologyContext context) {
        State state;
        try {
            String provider = null;
            if (stormConf.containsKey(Config.TOPOLOGY_STATE_PROVIDER)) {
                provider = (String) stormConf.get(Config.TOPOLOGY_STATE_PROVIDER);
            } else {
                provider = DEFAULT_PROVIDER;
            }
            Class<?> klazz = Class.forName(provider);
            Object object = klazz.newInstance();
            if (object instanceof StateProvider) {
                state = ((StateProvider) object).newState(namespace, stormConf, context);
            } else {
                String msg = "Invalid state provider '" + provider +
                        "'. Should implement org.apache.storm.state.StateProvider";
                LOG.error(msg);
                throw new RuntimeException(msg);
            }
        } catch (Exception ex) {
            LOG.error("Got exception while loading the state provider", ex);
            throw new RuntimeException(ex);
        }
        return state;
    }
}
