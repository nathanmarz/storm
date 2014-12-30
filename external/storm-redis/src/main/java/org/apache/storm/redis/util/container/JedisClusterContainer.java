package org.apache.storm.redis.util.container;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;

import java.io.Closeable;

public class JedisClusterContainer implements JedisCommandsInstanceContainer, Closeable {

    private JedisCluster jedisCluster;

    public JedisClusterContainer(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    @Override
    public JedisCommands getInstance() {
        return this.jedisCluster;
    }

    @Override
    public void returnInstance(JedisCommands jedisCommands) {
        // do nothing
    }

    @Override
    public void close() {
        this.jedisCluster.close();
    }
}
