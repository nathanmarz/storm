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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;

import java.io.Closeable;
import java.io.IOException;

/**
 * Container for managing Jedis instances.
 */
public class JedisContainer implements JedisCommandsInstanceContainer, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(JedisContainer.class);

    private JedisPool jedisPool;

    /**
     * Constructor
     * @param jedisPool JedisPool which actually manages Jedis instances
     */
    public JedisContainer(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JedisCommands getInstance() {
        return jedisPool.getResource();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void returnInstance(JedisCommands jedisCommands) {
        if (jedisCommands == null) {
            return;
        }

        try {
            ((Closeable) jedisCommands).close();
        } catch (IOException e) {
            LOG.error("Failed to close (return) instance to pool");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        jedisPool.close();
    }
}
