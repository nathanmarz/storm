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

import com.datastax.driver.core.Session;

public interface SimpleClient {

    /**
     * Creates a new session on this cluster.
     * *
     * @return a new session on this cluster.
     * @throws com.datastax.driver.core.exceptions.NoHostAvailableException if we cannot reach any cassandra contact points.
     */
    Session connect();

    /**
     * Close the underlying {@link com.datastax.driver.core.Cluster} instance.
     */
    void close();

    /**
     * Checks whether the underlying cluster instance is closed.
     */
    boolean isClose();
}
