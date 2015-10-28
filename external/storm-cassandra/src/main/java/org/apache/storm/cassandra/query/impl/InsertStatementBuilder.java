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
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Preconditions;
import org.apache.storm.cassandra.query.CQLStatementBuilder;
import org.apache.storm.cassandra.query.CQLTableTupleMapper;
import org.apache.storm.cassandra.query.CQLValuesTupleMapper;
import org.apache.storm.cassandra.query.SimpleCQLStatementTupleMapper;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InsertStatementBuilder<T extends CQLValuesTupleMapper> implements CQLStatementBuilder {
    /**
     * Optional name of the table into insert.
     */
    protected final String table;
    /**
     * Optional keyspace name to insert.
     */
    protected final String keyspace;
    /**
     * Optional mapper to retrieve a table name from tuple.
     */
    protected final CQLTableTupleMapper mapper2TableName;

    protected T valuesMapper;

    protected boolean ifNotExist;

    protected Integer ttlInSeconds;

    protected ConsistencyLevel level;

    /**
     * Creates a new {@link InsertStatementBuilder} instance.
     *
     * @param table    name of the table into insert.
     */
    public InsertStatementBuilder(String table) {
        this(null, table, null);
    }

    /**
     * Creates a new {@link InsertStatementBuilder} instance.
     *
     * @param table    name of the table into insert.
     * @param keyspace the keyspace's name to which the table belongs
     */
    public InsertStatementBuilder(String table, String keyspace) {
        this(keyspace, table, null);
    }

    /**
     * Creates a new {@link InsertStatementBuilder} instance.
     *
     * @param mapper2TableName the mapper that specify the table in which insert
     * @param keyspace         the name of the keyspace to which the table belongs
     */
    public InsertStatementBuilder(CQLTableTupleMapper mapper2TableName, String keyspace) {
        this(keyspace, null, mapper2TableName);
    }
    /**
     * Creates a new {@link InsertStatementBuilder} instance.
     *
     * @param mapper2TableName the mapper that specify the table in which insert
     */
    public InsertStatementBuilder(CQLTableTupleMapper mapper2TableName) {
        this(null, null, mapper2TableName);
    }

    private InsertStatementBuilder(String keyspace, String table, CQLTableTupleMapper mapper2TableName) {
        this.keyspace = keyspace;
        this.table = table;
        this.mapper2TableName = mapper2TableName;
    }

    /**
     * Adds "IF NOT EXISTS" clause to the statement.
     * @see com.datastax.driver.core.querybuilder.Insert#ifNotExists()
     */
    public InsertStatementBuilder ifNotExists() {
        this.ifNotExist = true;
        return this;
    }

    public InsertStatementBuilder usingTTL(Long time, TimeUnit unit) {
        this.ttlInSeconds = (int) unit.toSeconds(time);
        return this;
    }

    /**
     * Sets the consistency level used for this statement.
     *
     * @param level a ConsistencyLevel.
     */
    public InsertStatementBuilder withConsistencyLevel(ConsistencyLevel level) {
        this.level = level;
        return this;
    }

    /**
     * Maps tuple to values.
     *
     * @see com.datastax.driver.core.querybuilder.Insert#value(String, Object)
     */
    public InsertStatementBuilder values(final T valuesMapper) {
        this.valuesMapper = valuesMapper;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SimpleCQLStatementTupleMapper build() {
        Preconditions.checkState(null != table, "Table name must not be null");
        return new SimpleCQLStatementTupleMapper() {
            @Override
            public Statement map(ITuple tuple) {
                Insert stmt = QueryBuilder.insertInto(keyspace, table);
                for(Map.Entry<String, Object> entry : valuesMapper.map(tuple).entrySet())
                    stmt.value(entry.getKey(), entry.getValue());
                if (ttlInSeconds != null) stmt.using(QueryBuilder.ttl(ttlInSeconds));
                if (ifNotExist) stmt.ifNotExists();
                if (level != null) stmt.setConsistencyLevel(level);
                return stmt;
            }
        };
    }
}