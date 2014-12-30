package org.apache.storm.redis.util.config;

import com.google.common.base.Preconditions;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Protocol;

import java.io.Serializable;
import java.util.Set;

public class JedisClusterConfig implements Serializable {
    private Set<HostAndPort> nodes;
    private int timeout;
    private int maxRedirections;

    public JedisClusterConfig(Set<HostAndPort> nodes, int timeout, int maxRedirections) {
        this.nodes = nodes;
        this.timeout = timeout;
        this.maxRedirections = maxRedirections;
    }

    public Set<HostAndPort> getNodes() {
        return nodes;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getMaxRedirections() {
        return maxRedirections;
    }

    static class Builder {
        private Set<HostAndPort> nodes;
        private int timeout = Protocol.DEFAULT_TIMEOUT;
        private int maxRedirections = 5;

        public Builder setNodes(Set<HostAndPort> nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder setMaxRedirections(int maxRedirections) {
            this.maxRedirections = maxRedirections;
            return this;
        }

        public JedisClusterConfig build() {
            Preconditions.checkNotNull(this.nodes, "Node information should be presented");

            return new JedisClusterConfig(nodes, timeout, maxRedirections);
        }
    }
}
