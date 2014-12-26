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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisClusterState implements State {
    private static final Logger logger = LoggerFactory.getLogger(RedisClusterState.class);

    @Override
    public void beginCommit(Long aLong) {
    }

    @Override
    public void commit(Long aLong) {
    }

    public static class Options implements Serializable {
        public int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
        public int maxRedirections = 10;
    }

    public static class Factory implements StateFactory {
        Set<InetSocketAddress> clusterNodes;
        Options options;

        public Factory(Set<InetSocketAddress> clusterNodes, Options options) {
            this.clusterNodes = clusterNodes;
            this.options = options;
        }

        public State makeState(@SuppressWarnings("rawtypes") Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            Set<HostAndPort> nodes = new HashSet<HostAndPort>();
            for (InetSocketAddress addr : clusterNodes) {
                nodes.add(new HostAndPort(addr.getHostName(), addr.getPort()));
            }
            JedisCluster jc = new JedisCluster(nodes, options.connectionTimeout, options.maxRedirections);
            return new RedisClusterState(jc, options);
        }
    }

    private final JedisCluster jedisCluster;
    private Options options;

    public RedisClusterState(JedisCluster jedisCluster, Options options) {
        this.jedisCluster = jedisCluster;
        this.options = options;
    }

    /**
     * Set a timeout on the specified key. After the timeout the key will be
     * automatically deleted by the server. A key with an associated timeout is
     * said to be volatile in Redis terminology.
     * <p>
     * Voltile keys are stored on disk like the other keys, the timeout is
     * persistent too like all the other aspects of the dataset. Saving a
     * dataset containing expires and stopping the server does not stop the flow
     * of time as Redis stores on disk the time when the key will no longer be
     * available as Unix time, and not the remaining seconds.
     * <p>
     * Since Redis 2.1.3 you can update the value of the timeout of a key
     * already having an expire set. It is also possible to undo the expire at
     * all turning the key into a normal key using the {@link #persist(String)
     * PERSIST} command.
     * <p>
     * Time complexity: O(1)
     *
     * @see <ahref="http://code.google.com/p/redis/wiki/ExpireCommand">ExpireCommand</a>
     *
     * @param key
     * @param seconds
     * @return Integer reply, specifically: 1: the timeout was set. 0: the
     *         timeout was not set since the key already has an associated
     *         timeout (this may happen only in Redis versions < 2.1.3, Redis >=
     *         2.1.3 will happily update the timeout), or the key does not
     *         exist.
     */
    public Long expire(final String key, final int seconds) {
        return jedisCluster.expire(key, seconds);
    }

    /**
     * EXPIREAT works exctly like {@link #expire(String, int) EXPIRE} but
     * instead to get the number of seconds representing the Time To Live of the
     * key as a second argument (that is a relative way of specifing the TTL),
     * it takes an absolute one in the form of a UNIX timestamp (Number of
     * seconds elapsed since 1 Gen 1970).
     * <p>
     * EXPIREAT was introduced in order to implement the Append Only File
     * persistence mode so that EXPIRE commands are automatically translated
     * into EXPIREAT commands for the append only file. Of course EXPIREAT can
     * also used by programmers that need a way to simply specify that a given
     * key should expire at a given time in the future.
     * <p>
     * Since Redis 2.1.3 you can update the value of the timeout of a key
     * already having an expire set. It is also possible to undo the expire at
     * all turning the key into a normal key using the {@link #persist(String)
     * PERSIST} command.
     * <p>
     * Time complexity: O(1)
     *
     * @see <ahref="http://code.google.com/p/redis/wiki/ExpireCommand">ExpireCommand</a>
     *
     * @param key
     * @param unixTime
     * @return Integer reply, specifically: 1: the timeout was set. 0: the
     *         timeout was not set since the key already has an associated
     *         timeout (this may happen only in Redis versions < 2.1.3, Redis >=
     *         2.1.3 will happily update the timeout), or the key does not
     *         exist.
     */
    public Long expireAt(final String key, final long unixTime) {
        return jedisCluster.expireAt(key, unixTime);
    }

    /**
     * Undo a {@link #expire(String, int) expire} at turning the expire key into
     * a normal key.
     * <p>
     * Time complexity: O(1)
     *
     * @param key
     * @return Integer reply, specifically: 1: the key is now persist. 0: the
     *         key is not persist (only happens when key not set).
     */
    public Long persist(final String key) {
        return jedisCluster.persist(key);
    }

    public Long sadd(final String key, final String... member) {
        return jedisCluster.sadd(key, member);
    }

    public Set<String> smembers(final String key) {
        return jedisCluster.smembers(key);
    }

    public Long srem(final String key, final String... member) {
        return jedisCluster.srem(key, member);
    }

    public String spop(final String key) {
        return jedisCluster.spop(key);
    }

    public Long scard(final String key) {
        return jedisCluster.scard(key);
    }

    public Boolean sismember(final String key, final String member) {
        return jedisCluster.sismember(key, member);
    }

    public String srandmember(final String key) {
        return jedisCluster.srandmember(key);
    }

    public List<String> srandmember(final String key, final int count) {
        return jedisCluster.srandmember(key, count);
    }
}
