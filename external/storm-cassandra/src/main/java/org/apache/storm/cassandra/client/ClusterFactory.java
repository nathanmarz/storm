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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.cassandra.context.BaseBeanFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Default interface to build cassandra Cluster from the a Storm Topology configuration.
 */
public class ClusterFactory extends BaseBeanFactory<Cluster> {

    /**
     * Creates a new Cluster based on the specified configuration.
     * @param stormConf the storm configuration.
     * @return a new a new {@link com.datastax.driver.core.Cluster} instance.
     */
    @Override
    protected Cluster make(Map<String, Object> stormConf) {
        CassandraConf cassandraConf = new CassandraConf(stormConf);

        Cluster.Builder cluster = Cluster.builder()
                .withoutJMXReporting()
                .withoutMetrics()
                .addContactPoints(cassandraConf.getNodes())
                .withPort(cassandraConf.getPort())
                .withRetryPolicy(cassandraConf.getRetryPolicy())
                .withReconnectionPolicy(new ExponentialReconnectionPolicy(
                        cassandraConf.getReconnectionPolicyBaseMs(),
                        cassandraConf.getReconnectionPolicyMaxMs()))
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));

        final String username = cassandraConf.getUsername();
        final String password = cassandraConf.getPassword();

        if( StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            cluster.withAuthProvider(new PlainTextAuthProvider(username, password));
        }

        QueryOptions options = new QueryOptions()
                .setConsistencyLevel(cassandraConf.getConsistencyLevel());
        cluster.withQueryOptions(options);


        return cluster.build();
    }
}
