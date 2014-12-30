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
