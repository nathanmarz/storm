package org.apache.storm.redis.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import org.apache.storm.redis.util.config.JedisClusterConfig;
import org.apache.storm.redis.util.config.JedisPoolConfig;
import org.apache.storm.redis.util.container.JedisCommandsContainerBuilder;
import org.apache.storm.redis.util.container.JedisCommandsInstanceContainer;
import redis.clients.jedis.JedisCommands;

import java.util.Map;

public abstract class AbstractRedisBolt extends BaseRichBolt {
    protected OutputCollector collector;

    private transient JedisCommandsInstanceContainer container;

    private JedisPoolConfig jedisPoolConfig;
    private JedisClusterConfig jedisClusterConfig;

    public AbstractRedisBolt(JedisPoolConfig config) {
        this.jedisPoolConfig = config;
    }

    public AbstractRedisBolt(JedisClusterConfig config) {
        this.jedisClusterConfig = config;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;

        if (jedisPoolConfig != null) {
            this.container = JedisCommandsContainerBuilder.build(jedisPoolConfig);
        } else if (jedisClusterConfig != null) {
            this.container = JedisCommandsContainerBuilder.build(jedisClusterConfig);
        } else {
            throw new IllegalArgumentException("Jedis configuration not found");
        }
    }

    protected JedisCommands getInstance() {
        return this.container.getInstance();
    }

    protected void returnInstance(JedisCommands instance) {
        this.container.returnInstance(instance);
    }
}
