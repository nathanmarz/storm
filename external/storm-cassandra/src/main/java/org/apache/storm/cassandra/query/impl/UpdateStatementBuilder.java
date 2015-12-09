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
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import org.apache.storm.cassandra.query.CQLClauseTupleMapper;
import org.apache.storm.cassandra.query.CQLStatementBuilder;
import org.apache.storm.cassandra.query.CQLValuesTupleMapper;
import org.apache.storm.cassandra.query.SimpleCQLStatementTupleMapper;

import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

public final class UpdateStatementBuilder implements CQLStatementBuilder {

    /**
     * The name of table to update. 
     */
    private final String table;
    /**
     * The keyspace of the table.
     */
    private final String keyspace;

    private CQLValuesTupleMapper valuesMapper;

    private CQLClauseTupleMapper clausesMapper;

    private CQLClauseTupleMapper onlyIfClausesMapper;
    /**
     * Creates a new {@link UpdateStatementBuilder} instance.
     * @param table the name of table to update.
     */
    public UpdateStatementBuilder(String table) {
        this(table, null);
    }

    /**
     * Creates a new {@link UpdateStatementBuilder} instance.
     * @param table the name of table to update.
     * @param keyspace the keyspace of the table.
     */
    public UpdateStatementBuilder(String table, String keyspace) {
        this.table = table;
        this.keyspace = keyspace;
    }

    /**
     * Maps output tuple to values.
     * @see com.datastax.driver.core.querybuilder.Update#with(com.datastax.driver.core.querybuilder.Assignment)
     */
    public UpdateStatementBuilder with(final CQLValuesTupleMapper values) {
        this.valuesMapper = values;
        return this;
    }

    /**
     * Maps output tuple to some Where clauses.
     * @see com.datastax.driver.core.querybuilder.Update#where(com.datastax.driver.core.querybuilder.Clause)
     */
    public UpdateStatementBuilder where(final CQLClauseTupleMapper clauses) {
        this.clausesMapper = clauses;
        return this;
    }

    /**
     * Maps output tuple to some If clauses.
     * @see com.datastax.driver.core.querybuilder.Update#onlyIf(com.datastax.driver.core.querybuilder.Clause)
     */
    public UpdateStatementBuilder onlyIf(final CQLClauseTupleMapper clauses) {
        this.onlyIfClausesMapper = clauses;
        return this;
    }

    @Override
    public SimpleCQLStatementTupleMapper build() {
        return new SimpleCQLStatementTupleMapper() {
            @Override
            public Statement map(ITuple tuple) {
                Update stmt = QueryBuilder.update(keyspace, table);
                for(Map.Entry<String, Object> entry : valuesMapper.map(tuple).entrySet())
                    stmt.with(set(entry.getKey(), entry.getValue()));
                for(Clause clause : clausesMapper.map(tuple))
                    stmt.where(clause);
                if( hasIfClauses())
                    for(Clause clause : onlyIfClausesMapper.map(tuple)) {
                        stmt.onlyIf(clause);
                    }
                return stmt;
            }
        };
    }

    private boolean hasIfClauses() {
        return onlyIfClausesMapper != null;
    }
}