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
package org.apache.storm.redis.common.config;

import redis.clients.jedis.Protocol;

import java.io.Serializable;

/**
 * Configuration for JedisPool.
 */
public class JedisPoolConfig implements Serializable {
    public static final String DEFAULT_HOST = "127.0.0.1";

    private String host;
    private int port;
    private int timeout;
    private int database;
    private String password;

    // for serialization
    public JedisPoolConfig() {
    }
    /**
     * Constructor
     * <p/>
     * You can use JedisPoolConfig.Builder() for leaving some fields to apply default value.
     *
     * @param host hostname or IP
     * @param port port
     * @param timeout socket / connection timeout
     * @param database database index
     * @param password password, if any
     */
    public JedisPoolConfig(String host, int port, int timeout, String password, int database) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.database = database;
        this.password = password;
    }

    /**
     * Returns host.
     * @return hostname or IP
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns port.
     * @return port
     */
    public int getPort() {
        return port;
    }

    /**
     * Returns timeout.
     * @return socket / connection timeout
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * Returns database index.
     * @return database index
     */
    public int getDatabase() {
        return database;
    }

    /**
     * Returns password.
     * @return password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Builder for initializing JedisPoolConfig.
     */
    public static class Builder {
        private String host = DEFAULT_HOST;
        private int port = Protocol.DEFAULT_PORT;
        private int timeout = Protocol.DEFAULT_TIMEOUT;
        private int database = Protocol.DEFAULT_DATABASE;
        private String password;

        /**
         * Sets host.
         * @param host host
         * @return Builder itself
         */
        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        /**
         * Sets port.
         * @param port port
         * @return Builder itself
         */
        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        /**
         * Sets timeout.
         * @param timeout timeout
         * @return Builder itself
         */
        public Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Sets database index.
         * @param database database index
         * @return Builder itself
         */
        public Builder setDatabase(int database) {
            this.database = database;
            return this;
        }

        /**
         * Sets password.
         * @param password password, if any
         * @return Builder itself
         */
        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * Builds JedisPoolConfig.
         * @return JedisPoolConfig
         */
        public JedisPoolConfig build() {
            return new JedisPoolConfig(host, port, timeout, password, database);
        }
    }

    @Override
    public String toString() {
        return "JedisPoolConfig{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", timeout=" + timeout +
                ", database=" + database +
                ", password='" + password + '\'' +
                '}';
    }
}
