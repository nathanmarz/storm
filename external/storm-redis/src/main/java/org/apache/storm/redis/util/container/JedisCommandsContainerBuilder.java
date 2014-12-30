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
package org.apache.storm.redis.util.container;

import org.apache.storm.redis.util.config.JedisClusterConfig;
import org.apache.storm.redis.util.config.JedisPoolConfig;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

public class JedisCommandsContainerBuilder {

    // TODO : serialize redis.clients.jedis.JedisPoolConfig
    public static final redis.clients.jedis.JedisPoolConfig DEFAULT_POOL_CONFIG = new redis.clients.jedis.JedisPoolConfig();

    public static JedisCommandsInstanceContainer build(JedisPoolConfig config) {
        JedisPool jedisPool = new JedisPool(DEFAULT_POOL_CONFIG, config.getHost(), config.getPort(), config.getTimeout(), config.getPassword(), config.getDatabase());
        return new JedisContainer(jedisPool);
    }

    public static JedisCommandsInstanceContainer build(JedisClusterConfig config) {
        JedisCluster jedisCluster = new JedisCluster(config.getNodes(), config.getTimeout(), config.getMaxRedirections(), DEFAULT_POOL_CONFIG);
        return new JedisClusterContainer(jedisCluster);
    }
}
