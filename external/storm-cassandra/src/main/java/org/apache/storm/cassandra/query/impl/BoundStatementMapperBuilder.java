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

import backtype.storm.tuple.ITuple;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.apache.storm.cassandra.query.CQLValuesTupleMapper;
import org.apache.storm.cassandra.query.ContextQuery;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.storm.cassandra.query.ContextQuery.*;


public class BoundStatementMapperBuilder implements Serializable {
    private final ContextQuery contextQuery;

    /**
     * Creates a new {@link BoundStatementMapperBuilder} instance.
     * @param cql
     */
    public BoundStatementMapperBuilder(String cql) {
        this.contextQuery = new StaticContextQuery(cql);
    }

    /**
     * Creates a new {@link BoundStatementMapperBuilder} instance.
     * @param contextQuery
     */
    public BoundStatementMapperBuilder(ContextQuery contextQuery) {
        this.contextQuery = contextQuery;
    }

    public CQLStatementTupleMapper bind(final CQLValuesTupleMapper mapper) {
        return new CQLBoundStatementTupleMapper(contextQuery, mapper);
    }

    public static class CQLBoundStatementTupleMapper implements CQLStatementTupleMapper {

        private final ContextQuery contextQuery;

        private final CQLValuesTupleMapper mapper;

        private Map<String, PreparedStatement> cache = new HashMap<>();

        /**
         * Creates a new {@link CQLBoundStatementTupleMapper} instance.
         *
         * @param contextQuery
         * @param mapper
         */
        CQLBoundStatementTupleMapper(ContextQuery contextQuery, CQLValuesTupleMapper mapper) {
            this.contextQuery = contextQuery;
            this.mapper = mapper;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public List<Statement> map(Map config, Session session, ITuple tuple) {
            final Map<String, Object> values = mapper.map(tuple);
            final String query = contextQuery.resolves(config, tuple);
            Object[] objects = values.values().toArray(new Object[values.size()]);
            PreparedStatement statement = getPreparedStatement(session, query);
            return Arrays.asList((Statement)statement.bind(objects));
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
}
