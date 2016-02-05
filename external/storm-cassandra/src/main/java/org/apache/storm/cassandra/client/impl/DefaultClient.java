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
package org.apache.storm.cassandra.client.impl;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.base.Preconditions;
import org.apache.storm.cassandra.client.SimpleClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Set;

/**
 * Simple class to wrap cassandra {@link com.datastax.driver.core.Cluster} instance.
 */
public class DefaultClient implements SimpleClient, Closeable, Serializable {

    private final static Logger LOG = LoggerFactory.getLogger(DefaultClient.class);

    private String keyspace;

    private Cluster cluster;

    private Session session;

    /**
     * Create a new {@link DefaultClient} instance.
     *
     * @param cluster a cassandra cluster client.
     */
    public DefaultClient(Cluster cluster, String keyspace) {
        Preconditions.checkNotNull(cluster, "Cluster cannot be 'null");
        this.cluster = cluster;
        this.keyspace = keyspace;

    }

    public Set<Host> getAllHosts() {
        Metadata metadata = getMetadata();
        return metadata.getAllHosts();
    }

    public Metadata getMetadata() {
        return cluster.getMetadata();
    }


    private String getExecutorName() {
        Thread thread = Thread.currentThread();
        return thread.getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Session connect() throws NoHostAvailableException {
        if (isDisconnected()) {
            LOG.info("Connected to cluster: {}", cluster.getClusterName());
            for (Host host : getAllHosts())
                LOG.info("Datacenter: {}; Host: {}; Rack: {}", host.getDatacenter(), host.getAddress(), host.getRack());

            LOG.info("Connect to cluster using keyspace %s", keyspace);
            session = cluster.connect(keyspace);
        } else {
            LOG.warn("{} - Already connected to cluster: {}", getExecutorName(), cluster.getClusterName());
        }

        if (session.isClosed()) {
            LOG.warn("Session has been closed - create new one!");
            this.session = cluster.newSession();
        }
        return session;
    }

    /**
     * Checks whether the client is already connected to the cluster.
     */
    protected boolean isDisconnected() {
        return session == null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (cluster != null && !cluster.isClosed()) {
            LOG.info("Try to close connection to cluster: {}", cluster.getClusterName());
            session.close();
            cluster.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClose() {
        return this.cluster.isClosed();
    }

}
