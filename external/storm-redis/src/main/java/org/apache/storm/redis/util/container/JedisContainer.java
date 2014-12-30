package org.apache.storm.redis.util.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;

import java.io.Closeable;
import java.io.IOException;

public class JedisContainer implements JedisCommandsInstanceContainer, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(JedisContainer.class);

    private JedisPool jedisPool;

    public JedisContainer(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public JedisCommands getInstance() {
        return jedisPool.getResource();
    }

    @Override
    public void returnInstance(JedisCommands jedisCommands) {
        try {
            ((Closeable) jedisCommands).close();
        } catch (IOException e) {
            LOG.warn("Failed to close (return) instance to pool");
            try {
                jedisPool.returnBrokenResource((Jedis) jedisCommands);
            } catch (Exception e2) {
                LOG.error("Failed to discard instance from pool");
            }
        }
    }

    @Override
    public void close() {
        jedisPool.close();
    }
}
