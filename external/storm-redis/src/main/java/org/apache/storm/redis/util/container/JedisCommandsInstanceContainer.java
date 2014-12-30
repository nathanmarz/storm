package org.apache.storm.redis.util.container;

import redis.clients.jedis.JedisCommands;

public interface JedisCommandsInstanceContainer {
    JedisCommands getInstance();
    void returnInstance(JedisCommands jedisCommands);
}
