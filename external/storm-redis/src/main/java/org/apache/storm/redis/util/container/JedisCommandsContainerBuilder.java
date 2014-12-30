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
