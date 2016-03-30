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

package org.apache.storm.hive.common;

import java.io.Serializable;

import org.apache.storm.hive.common.HiveWriter;
import org.apache.storm.hive.bolt.mapper.HiveMapper;
import org.apache.hive.hcatalog.streaming.*;


public class HiveOptions implements Serializable {
    /**
     * Half of the default Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS
     */
    private static final int DEFAULT_TICK_TUPLE_INTERVAL_SECS = 15;

    protected HiveMapper mapper;
    protected String databaseName;
    protected String tableName;
    protected String metaStoreURI;
    protected Integer txnsPerBatch = 100;
    protected Integer maxOpenConnections = 10;
    protected Integer batchSize = 15000;
    protected Integer idleTimeout = 60000;
    protected Integer callTimeout = 0;
    protected Integer heartBeatInterval = 60;
    protected Boolean autoCreatePartitions = true;
    protected String kerberosPrincipal;
    protected String kerberosKeytab;
    protected Integer tickTupleInterval = DEFAULT_TICK_TUPLE_INTERVAL_SECS;

    public HiveOptions(String metaStoreURI,String databaseName,String tableName,HiveMapper mapper) {
        this.metaStoreURI = metaStoreURI;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.mapper = mapper;
    }

    public HiveOptions withTickTupleInterval(Integer tickInterval)
    {
        this.tickTupleInterval = tickInterval;
        return this;
    }

    public HiveOptions withTxnsPerBatch(Integer txnsPerBatch) {
        this.txnsPerBatch = txnsPerBatch;
        return this;
    }

    public HiveOptions withMaxOpenConnections(Integer maxOpenConnections) {
        this.maxOpenConnections = maxOpenConnections;
        return this;
    }

    public HiveOptions withBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public HiveOptions withIdleTimeout(Integer idleTimeout) {
        this.idleTimeout = idleTimeout;
        return this;
    }

    public HiveOptions withCallTimeout(Integer callTimeout) {
        this.callTimeout = callTimeout;
        return this;
    }

    public HiveOptions withHeartBeatInterval(Integer heartBeatInterval) {
        this.heartBeatInterval = heartBeatInterval;
        return this;
    }

    public HiveOptions withAutoCreatePartitions(Boolean autoCreatePartitions) {
        this.autoCreatePartitions = autoCreatePartitions;
        return this;
    }

    public HiveOptions withKerberosKeytab(String kerberosKeytab) {
        this.kerberosKeytab = kerberosKeytab;
        return this;
    }

    public HiveOptions withKerberosPrincipal(String kerberosPrincipal) {
        this.kerberosPrincipal = kerberosPrincipal;
        return this;
    }

    public String getMetaStoreURI() {
        return metaStoreURI;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public HiveMapper getMapper() {
        return mapper;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public Integer getCallTimeOut() {
        return callTimeout;
    }

    public Integer getHeartBeatInterval() {
        return heartBeatInterval;
    }

    public Integer getMaxOpenConnections() {
        return maxOpenConnections;
    }

    public Integer getIdleTimeout() {
        return idleTimeout;
    }

    public Integer getTxnsPerBatch() {
        return txnsPerBatch;
    }

    public Boolean getAutoCreatePartitions() {
        return autoCreatePartitions;
    }

    public String getKerberosPrincipal() {
        return kerberosPrincipal;
    }

    public String getKerberosKeytab() {
        return kerberosKeytab;
    }

    public Integer getTickTupleInterval() {
        return tickTupleInterval;
    }
}
