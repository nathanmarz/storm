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
package org.apache.storm.redis.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import org.apache.storm.redis.util.config.JedisClusterConfig;
import org.apache.storm.redis.util.config.JedisPoolConfig;
import org.apache.storm.redis.util.container.JedisCommandsContainerBuilder;
import org.apache.storm.redis.util.container.JedisCommandsInstanceContainer;
import redis.clients.jedis.JedisCommands;

import java.util.Map;

public abstract class AbstractRedisBolt extends BaseRichBolt {
    protected OutputCollector collector;

    private transient JedisCommandsInstanceContainer container;

    private JedisPoolConfig jedisPoolConfig;
    private JedisClusterConfig jedisClusterConfig;

    public AbstractRedisBolt(JedisPoolConfig config) {
        this.jedisPoolConfig = config;
    }

    public AbstractRedisBolt(JedisClusterConfig config) {
        this.jedisClusterConfig = config;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;

        if (jedisPoolConfig != null) {
            this.container = JedisCommandsContainerBuilder.build(jedisPoolConfig);
        } else if (jedisClusterConfig != null) {
            this.container = JedisCommandsContainerBuilder.build(jedisClusterConfig);
        } else {
            throw new IllegalArgumentException("Jedis configuration not found");
        }
    }

    protected JedisCommands getInstance() {
        return this.container.getInstance();
    }

    protected void returnInstance(JedisCommands instance) {
        this.container.returnInstance(instance);
    }
}
