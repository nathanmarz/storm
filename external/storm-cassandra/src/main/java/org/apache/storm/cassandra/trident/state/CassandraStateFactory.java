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
package org.apache.storm.cassandra.trident.state;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.cassandra.CassandraContext;
import org.apache.storm.cassandra.query.CQLResultSetValuesMapper;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import java.util.Map;

/**
 *
 */
public class CassandraStateFactory implements StateFactory {
    private final CassandraState.Options options;

    public CassandraStateFactory(CassandraState.Options options) {
        this.options = options;
    }

    public CassandraStateFactory(CQLStatementTupleMapper cqlStatementTupleMapper, CQLResultSetValuesMapper cqlResultSetValuesMapper) {
        this(new CassandraState.Options(new CassandraContext()).withCQLStatementTupleMapper(cqlStatementTupleMapper).withCQLResultSetValuesMapper(cqlResultSetValuesMapper));
    }

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        CassandraState cassandraState = new CassandraState(conf, options);
        cassandraState.prepare();

        return cassandraState;
    }
}
