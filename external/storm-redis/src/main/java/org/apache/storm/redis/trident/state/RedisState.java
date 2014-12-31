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
package org.apache.storm.redis.trident.state;

import backtype.storm.task.IMetricsContext;
import org.apache.storm.redis.util.config.JedisClusterConfig;
import org.apache.storm.redis.util.config.JedisPoolConfig;
import org.apache.storm.redis.util.container.JedisCommandsContainerBuilder;
import org.apache.storm.redis.util.container.JedisCommandsInstanceContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

public class RedisState implements State {
    private static final Logger logger = LoggerFactory.getLogger(RedisState.class);

    @Override
    public void beginCommit(Long aLong) {
    }

    @Override
    public void commit(Long aLong) {
    }

    public static class Factory implements StateFactory {
        private JedisPoolConfig jedisPoolConfig;
        private JedisClusterConfig jedisClusterConfig;

        public Factory(JedisPoolConfig config) {
            this.jedisPoolConfig = config;
        }

        public Factory(JedisClusterConfig config) {
            this.jedisClusterConfig = config;
        }

        public State makeState(@SuppressWarnings("rawtypes") Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            JedisCommandsInstanceContainer container;
            if (jedisPoolConfig != null) {
                container = JedisCommandsContainerBuilder.build(jedisPoolConfig);
            } else if (jedisClusterConfig != null) {
                container = JedisCommandsContainerBuilder.build(jedisClusterConfig);
            } else {
                throw new IllegalArgumentException("Jedis configuration not found");
            }

            return new RedisState(container);
        }
    }

    private transient JedisCommandsInstanceContainer container;

    public RedisState(JedisCommandsInstanceContainer container) {
        this.container = container;
    }

    /**
     * The state updater and querier can get a JedisCommands instance
     * */
    public JedisCommands getInstance() {
        return this.container.getInstance();
    }

    /**
     * The state updater and querier return the JedisCommands instance
     * */
    public void returnInstance(JedisCommands instance) {
        this.container.returnInstance(instance);
    }

}
