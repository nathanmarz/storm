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

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Implementation of State for single Redis environment.
 */
public class RedisState implements State {
    /**
     * {@inheritDoc}
     */
    @Override
    public void beginCommit(Long aLong) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit(Long aLong) {
    }

    /**
     * RedisState.Factory implements StateFactory for single Redis environment.
     *
     * @see StateFactory
     */
    public static class Factory implements StateFactory {
        public static final redis.clients.jedis.JedisPoolConfig DEFAULT_POOL_CONFIG = new redis.clients.jedis.JedisPoolConfig();

        private JedisPoolConfig jedisPoolConfig;

        /**
         * Constructor
         *
         * @param config configuration of JedisPool
         */
        public Factory(JedisPoolConfig config) {
            this.jedisPoolConfig = config;
        }

        /**
         * {@inheritDoc}
         */
        @Override
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

    /**
     * Constructor
     *
     * @param jedisPool JedisPool
     */
    public RedisState(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    /**
     * Borrows Jedis instance from pool.
     * <p/>
     * Note that you should return borrowed instance to pool when you finish using instance.
     *
     * @return Jedis instance
     */
    public Jedis getJedis() {
        return this.jedisPool.getResource();
    }

    /**
     * Returns Jedis instance to pool.
     *
     * @param jedis Jedis instance to return to pool
     */
    public void returnJedis(Jedis jedis) {
        jedis.close();
    }

}
