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

import org.apache.storm.utils.Utils;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
    public static final String CASSANDRA_RETRY_POLICY       = "cassandra.retryPolicy";
    public static final String CASSANDRA_RECONNECT_POLICY_BASE_MS  = "cassandra.reconnectionPolicy.baseDelayMs";
    public static final String CASSANDRA_RECONNECT_POLICY_MAX_MS   = "cassandra.reconnectionPolicy.maxDelayMs";

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
     * The retry policy to use for the new cluster.
     */
    private String retryPolicyName;

    /**
     * The base delay in milliseconds to use for the reconnection policy.
     */
    private long reconnectionPolicyBaseMs;

    /**
     * The maximum delay to wait between two attempts.
     */
    private long reconnectionPolicyMaxMs;
    
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

        this.username = (String)Utils.get(conf, CASSANDRA_USERNAME, null);
        this.password = (String)Utils.get(conf, CASSANDRA_PASSWORD, null);
        this.keyspace = get(conf, CASSANDRA_KEYSPACE);
        this.consistencyLevel = ConsistencyLevel.valueOf((String)Utils.get(conf, CASSANDRA_CONSISTENCY_LEVEL, ConsistencyLevel.ONE.name()));
        this.nodes    = ((String)Utils.get(conf, CASSANDRA_NODES, "localhost")).split(",");
        this.batchSizeRows = Utils.getInt(conf.get(CASSANDRA_BATCH_SIZE_ROWS), 100);
        this.port = Utils.getInt(conf.get(CASSANDRA_PORT), 9042);
        this.retryPolicyName = (String)Utils.get(conf, CASSANDRA_RETRY_POLICY, DefaultRetryPolicy.class.getSimpleName());
        this.reconnectionPolicyBaseMs = getLong(conf.get(CASSANDRA_RECONNECT_POLICY_BASE_MS), 100L);
        this.reconnectionPolicyMaxMs  = getLong(conf.get(CASSANDRA_RECONNECT_POLICY_MAX_MS), TimeUnit.MINUTES.toMillis(1));
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

    public long getReconnectionPolicyBaseMs() {
        return reconnectionPolicyBaseMs;
    }

    public long getReconnectionPolicyMaxMs() {
        return reconnectionPolicyMaxMs;
    }

    public RetryPolicy getRetryPolicy() {
        if(this.retryPolicyName.equals(DowngradingConsistencyRetryPolicy.class.getSimpleName()))
            return DowngradingConsistencyRetryPolicy.INSTANCE;
        if(this.retryPolicyName.equals(FallthroughRetryPolicy.class.getSimpleName()))
            return FallthroughRetryPolicy.INSTANCE;
        if(this.retryPolicyName.equals(DefaultRetryPolicy.class.getSimpleName()))
            return DefaultRetryPolicy.INSTANCE;
        throw new IllegalArgumentException("Unknown cassandra retry policy " + this.retryPolicyName);
    }

    private <T> T get(Map<String, Object> conf, String key) {
        Object o = conf.get(key);
        if(o == null) {
            throw new IllegalArgumentException("No '" + key + "' value found in configuration!");
        }
        return (T)o;
    }

    public static Long getLong(Object o, Long defaultValue) {
        if (null == o) {
            return defaultValue;
        }
        if (o instanceof Number) {
            return ((Number) o).longValue();
        } else if (o instanceof String) {
            return Long.parseLong((String) o);
        }
        throw new IllegalArgumentException("Don't know how to convert " + o + " to long");
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
                .add("retryPolicyName", retryPolicyName)
                .add("reconnectionPolicyBaseMs", reconnectionPolicyBaseMs)
                .add("reconnectionPolicyMaxMs", reconnectionPolicyMaxMs)
                .toString();
    }
}
