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
import org.apache.storm.redis.util.config.JedisPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
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
        public static final redis.clients.jedis.JedisPoolConfig DEFAULT_POOL_CONFIG = new redis.clients.jedis.JedisPoolConfig();

        private JedisPoolConfig jedisPoolConfig;

        public Factory(JedisPoolConfig config) {
            this.jedisPoolConfig = config;
        }

        public State makeState(@SuppressWarnings("rawtypes") Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            JedisPool jedisPool = new JedisPool(DEFAULT_POOL_CONFIG,
                                                jedisPoolConfig.getHost(),
                                                jedisPoolConfig.getPort(),
                                                jedisPoolConfig.getTimeout(),
                                                jedisPoolConfig.getPassword(),
                                                jedisPoolConfig.getDatabase());

            return new RedisState(jedisPool);
        }
    }

    private JedisPool jedisPool;

    public RedisState(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    /**
     * The state updater and querier can get a Jedis instance
     * */
    public Jedis getJedis() {
        return this.jedisPool.getResource();
    }

    /**
     * The state updater and querier return the Jedis instance
     * */
    public void returnJedis(Jedis jedis) {
        jedis.close();
    }

}
