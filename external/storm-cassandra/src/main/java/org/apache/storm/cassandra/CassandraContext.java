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
package org.apache.storm.cassandra;

import com.datastax.driver.core.Cluster;
import org.apache.storm.cassandra.client.CassandraConf;
import org.apache.storm.cassandra.client.ClusterFactory;
import org.apache.storm.cassandra.client.SimpleClient;
import org.apache.storm.cassandra.client.SimpleClientProvider;
import org.apache.storm.cassandra.client.impl.DefaultClient;
import org.apache.storm.cassandra.context.BaseBeanFactory;
import org.apache.storm.cassandra.context.WorkerCtx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *
 */
public class CassandraContext extends WorkerCtx implements SimpleClientProvider {

    /**
     * Creates a new {@link CassandraContext} instance.
     */
    public CassandraContext() {
        register(SimpleClient.class, new ClientFactory());
        register(CassandraConf.class, new CassandraConfFactory());
        register(Cluster.class, new ClusterFactory());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SimpleClient getClient(Map<String, Object> config) {
        SimpleClient client = getWorkerBean(SimpleClient.class, config);
        if (client.isClose() )
            client = getWorkerBean(SimpleClient.class, config, true);
        return client;
    }

    /**
     * Simple class to make {@link CassandraConf} from a Storm topology configuration.
     */
    public static final class CassandraConfFactory extends BaseBeanFactory<CassandraConf> {
        /**
         * {@inheritDoc}
         */
        @Override
        protected CassandraConf make(Map<String, Object> stormConf) {
            return new CassandraConf(stormConf);
        }
    }

    /**
     * Simple class to make {@link ClientFactory} from a Storm topology configuration.
     */
    public static final class ClientFactory extends BaseBeanFactory<SimpleClient> {

        private static final Logger LOG = LoggerFactory.getLogger(ClientFactory.class);
        /**
         * {@inheritDoc}
         */
        @Override
        protected SimpleClient make(Map<String, Object> stormConf) {
            Cluster cluster = this.context.getWorkerBean(Cluster.class, stormConf);
            if( cluster.isClosed() ) {
                LOG.warn("Cluster is closed - trigger new initialization!");
                cluster = this.context.getWorkerBean(Cluster.class, stormConf, true);
            }
            CassandraConf config = this.context.getWorkerBean(CassandraConf.class, stormConf);
            return new DefaultClient(cluster, config.getKeyspace());
        }
    }
}
