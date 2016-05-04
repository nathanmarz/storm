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
package org.apache.storm.cassandra.query.impl;

import org.apache.storm.tuple.ITuple;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.apache.storm.cassandra.query.Column;
import org.apache.storm.cassandra.query.CqlMapper;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SimpleCQLStatementMapper implements CQLStatementTupleMapper {

    private final String queryString;
    private final CqlMapper mapper;
    private final RoutingKeyGenerator rkGenerator;

    /**
     * Creates a new {@link SimpleCQLStatementMapper} instance.
     * @param queryString the cql query string to execute.
     * @param mapper the mapper.
     */
    public SimpleCQLStatementMapper(String queryString, CqlMapper mapper) {
        this(queryString, mapper, null);
    }

    /**
     * Creates a new {@link SimpleCQLStatementMapper} instance.
     * @param queryString the cql query string to execute.
     * @param mapper the mapper.
     */
    public SimpleCQLStatementMapper(String queryString, CqlMapper mapper, RoutingKeyGenerator rkGenerator) {
        Preconditions.checkNotNull(queryString, "Query string must not be null");
        Preconditions.checkNotNull(mapper, "Mapper should not be null");
        this.queryString = queryString;
        this.mapper = mapper;
        this.rkGenerator = rkGenerator;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<Statement> map(Map conf, Session session, ITuple tuple) {
        List<Column> columns = mapper.map(tuple);
        SimpleStatement statement = new SimpleStatement(queryString, Column.getVals(columns));

        if(hasRoutingKeys()) {
            List<ByteBuffer> keys = rkGenerator.getRoutingKeys(tuple);
            if( keys.size() == 1)
                statement.setRoutingKey(keys.get(0));
            else
                statement.setRoutingKey(keys.toArray(new ByteBuffer[keys.size()]));
        }

        return Arrays.asList((Statement) statement);
    }

    private boolean hasRoutingKeys() {
        return rkGenerator != null;
    }
}
