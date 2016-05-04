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
package org.apache.storm.redis.common.container;

import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

/**
 * Container Builder which helps abstraction of two env. - single instance or Redis Cluster.
 */
public class JedisCommandsContainerBuilder {

    // FIXME: We're using default config since it cannot be serialized
    // We still needs to provide some options externally
    public static final redis.clients.jedis.JedisPoolConfig DEFAULT_POOL_CONFIG = new redis.clients.jedis.JedisPoolConfig();

    /**
     * Builds container for single Redis environment.
     * @param config configuration for JedisPool
     * @return container for single Redis environment
     */
    public static JedisCommandsInstanceContainer build(JedisPoolConfig config) {
        JedisPool jedisPool = new JedisPool(DEFAULT_POOL_CONFIG, config.getHost(), config.getPort(), config.getTimeout(), config.getPassword(), config.getDatabase());
        return new JedisContainer(jedisPool);
    }

    /**
     * Builds container for Redis Cluster environment.
     * @param config configuration for JedisCluster
     * @return container for Redis Cluster environment
     */
    public static JedisCommandsInstanceContainer build(JedisClusterConfig config) {
        JedisCluster jedisCluster = new JedisCluster(config.getNodes(), config.getTimeout(), config.getMaxRedirections(), DEFAULT_POOL_CONFIG);
        return new JedisClusterContainer(jedisCluster);
    }
}
