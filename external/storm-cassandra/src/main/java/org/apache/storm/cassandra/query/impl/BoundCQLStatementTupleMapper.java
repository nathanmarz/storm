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
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.apache.storm.cassandra.query.Column;
import org.apache.storm.cassandra.query.CqlMapper;
import org.apache.storm.cassandra.query.ContextQuery;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BoundCQLStatementTupleMapper implements CQLStatementTupleMapper {

    private final ContextQuery contextQuery;

    private final CqlMapper mapper;

    private Map<String, PreparedStatement> cache = new HashMap<>();

    private final RoutingKeyGenerator rkGenerator;

    private final PreparedStatementBinder binder;

    /**
     * Creates a new {@link BoundCQLStatementTupleMapper} instance.
     *
     * @param contextQuery
     * @param mapper
     * @param mapper
     * @param rkGenerator
     * @param binder
     */
    public BoundCQLStatementTupleMapper(ContextQuery contextQuery, CqlMapper mapper, RoutingKeyGenerator rkGenerator, PreparedStatementBinder binder) {
        Preconditions.checkNotNull(contextQuery, "ContextQuery must not be null");
        Preconditions.checkNotNull(mapper, "Mapper should not be null");
        this.contextQuery = contextQuery;
        this.mapper = mapper;
        this.rkGenerator = rkGenerator;
        this.binder = binder;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Statement> map(Map config, Session session, ITuple tuple) {
        final List<Column> columns = mapper.map(tuple);

        final String query = contextQuery.resolves(config, tuple);

        PreparedStatement statement = getPreparedStatement(session, query);
        if(hasRoutingKeys()) {
            List<ByteBuffer> keys = rkGenerator.getRoutingKeys(tuple);
            if( keys.size() == 1)
                statement.setRoutingKey(keys.get(0));
            else
                statement.setRoutingKey(keys.toArray(new ByteBuffer[keys.size()]));
        }

        return Arrays.asList((Statement) this.binder.apply(statement, columns));
    }

    private boolean hasRoutingKeys() {
        return rkGenerator != null;
    }

    /**
     * Get or prepare a statement using the specified session and the query.
     * *
     * @param session The cassandra session.
     * @param query The CQL query to prepare.
     */
    private PreparedStatement getPreparedStatement(Session session, String query) {
        PreparedStatement statement = cache.get(query);
        if( statement == null) {
            statement = session.prepare(query);
            cache.put(query, statement);
        }
        return statement;
    }
}
