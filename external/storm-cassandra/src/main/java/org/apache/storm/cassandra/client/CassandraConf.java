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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.cassandra.client;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.Map;

/**
 * Configuration used by cassandra storm components.
 */
public class CassandraConf implements Serializable {
    
    public static final String CASSANDRA_USERNAME           = "cassandra.username";
    public static final String CASSANDRA_PASSWORD           = "cassandra.password";
    public static final String CASSANDRA_KEYSPACE           = "cassandra.keyspace";
    public static final String CASSANDRA_CONSISTENCY_LEVEL  = "cassandra.output.consistencyLevel";
    public static final String CASSANDRA_NODES              = "cassandra.nodes";
    public static final String CASSANDRA_PORT               = "cassandra.port";
    public static final String CASSANDRA_BATCH_SIZE_ROWS    = "cassandra.batch.size.rows";

    /**
     * The authorized cassandra username.
     */
    private String username;
    /**
     * The authorized cassandra password
     */
    private String password;
    /**
     * The cassandra keyspace.
     */
    private String keyspace;
    /**
     * List of contacts nodes.
     */
    private String[] nodes = {"localhost"};

    /**
     * The port used to connect to nodes.
     */
    private int port = 9092;

    /**
     * Consistency level used to write statements.
     */
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
    /**
     * The maximal numbers of rows per batch.
     */
    private int batchSizeRows       = 100;
    
    /**
     * Creates a new {@link CassandraConf} instance.
     */
    public CassandraConf() {
        super();
    }

    /**
     * Creates a new {@link CassandraConf} instance.
     *
     * @param conf The storm configuration.
     */
    public CassandraConf(Map<String, Object> conf) {
        this.username = getOrElse(conf, CASSANDRA_USERNAME, null);
        this.password = getOrElse(conf, CASSANDRA_PASSWORD, null);
        this.keyspace = get(conf, CASSANDRA_KEYSPACE);
        this.consistencyLevel = ConsistencyLevel.valueOf(getOrElse(conf, CASSANDRA_CONSISTENCY_LEVEL, ConsistencyLevel.ONE.name()));
        this.nodes    = getOrElse(conf, CASSANDRA_NODES, "localhost").split(",");
        this.batchSizeRows = getOrElse(conf, CASSANDRA_BATCH_SIZE_ROWS, 100);
        this.port = conf.get(CASSANDRA_PORT) != null ? Integer.valueOf((String)conf.get(CASSANDRA_PORT)) : 9042;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String[] getNodes() {
        return nodes;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public int getBatchSizeRows() {
        return batchSizeRows;
    }

    public int getPort() {
        return this.port;
    }

    private <T> T get(Map<String, Object> conf, String key) {
        Object o = conf.get(key);
        if(o == null) {
            throw new IllegalArgumentException("No '" + key + "' value found in configuration!");
        }
        return (T)o;
    }

    private <T> T getOrElse(Map<String, Object> conf, String key, T def) {
        T o = (T) conf.get(key);
        return (o == null) ? def : o;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("username", username)
                .add("password", password)
                .add("keyspace", keyspace)
                .add("nodes", nodes)
                .add("port", port)
                .add("consistencyLevel", consistencyLevel)
                .add("batchSizeRows", batchSizeRows)
                .toString();
    }
}
