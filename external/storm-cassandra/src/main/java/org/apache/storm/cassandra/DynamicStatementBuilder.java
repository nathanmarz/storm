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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import org.apache.storm.cassandra.query.CQLStatementBuilder;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.apache.storm.cassandra.query.ContextQuery;
import org.apache.storm.cassandra.query.CqlMapper;
import org.apache.storm.cassandra.query.impl.BatchCQLStatementTupleMapper;
import org.apache.storm.cassandra.query.builder.BoundCQLStatementMapperBuilder;
import org.apache.storm.cassandra.query.builder.SimpleCQLStatementMapperBuilder;
import org.apache.storm.cassandra.query.selector.FieldSelector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DynamicStatementBuilder implements Serializable {

    private DynamicStatementBuilder() {
    }

    public static final SimpleCQLStatementMapperBuilder simpleQuery(String queryString) {
        return new SimpleCQLStatementMapperBuilder(queryString);
    }

    public static final SimpleCQLStatementMapperBuilder simpleQuery(BuiltStatement builtStatement) {
        return new SimpleCQLStatementMapperBuilder(builtStatement);
    }

    /**
     * Builds a new bound statement based on the specified query.
     *
     * @param cql the query.
     * @return a new {@link org.apache.storm.cassandra.query.builder.BoundCQLStatementMapperBuilder} instance.
     */
    public static final BoundCQLStatementMapperBuilder boundQuery(String cql) {
        return new BoundCQLStatementMapperBuilder(cql);
    }

    /**
     * Builds a new bound statement identified by the given field.
     *
     * @param field a context used to resolve the cassandra query.
     * @return a new {@link org.apache.storm.cassandra.query.builder.BoundCQLStatementMapperBuilder} instance.
     */
    public static final BoundCQLStatementMapperBuilder boundQuery(ContextQuery field) {
        return new BoundCQLStatementMapperBuilder(field);
    }

    /**
     * Builds multiple statements which will be executed asynchronously.
     *
     * @param builders a list of {@link CQLStatementBuilder}.
     * @return a new {@link CQLStatementTupleMapper}.
     */
    public static final CQLStatementTupleMapper async(final CQLStatementBuilder... builders) {
        return new CQLStatementTupleMapper.DynamicCQLStatementTupleMapper(Arrays.asList(builders));
    }

    /**
     * Creates a new {@link com.datastax.driver.core.BatchStatement.Type#LOGGED} batch statement for the specified CQL statement builders.
     */
    public static final BatchCQLStatementTupleMapper loggedBatch(CQLStatementBuilder... builders) {
        return newBatchStatementBuilder(BatchStatement.Type.LOGGED, builders);
    }
    /**
     * Creates a new {@link com.datastax.driver.core.BatchStatement.Type#COUNTER} batch statement for the specified CQL statement builders.
     */
    public static final BatchCQLStatementTupleMapper counterBatch(CQLStatementBuilder... builders) {
        return newBatchStatementBuilder(BatchStatement.Type.COUNTER, builders);
    }
    /**
     * Creates a new {@link com.datastax.driver.core.BatchStatement.Type#UNLOGGED} batch statement for the specified CQL statement builders.
     */
    public static final BatchCQLStatementTupleMapper unLoggedBatch(CQLStatementBuilder... builders) {
        return newBatchStatementBuilder(BatchStatement.Type.UNLOGGED, builders);
    }

    private static BatchCQLStatementTupleMapper newBatchStatementBuilder(BatchStatement.Type type, CQLStatementBuilder[] builders) {
        List<CQLStatementTupleMapper> mappers = new ArrayList<>(builders.length);
        for(CQLStatementBuilder b : Arrays.asList(builders))
            mappers.add(b.build());
        return new BatchCQLStatementTupleMapper(type, mappers);
    }

    /**
     * Retrieves from the storm configuration the specified named query.
     *
     * @param name query's name.
     */
    public static final ContextQuery named(final String name) {
        return new ContextQuery.BoundQueryContext(name);
    }

    /**
     * Retrieves from the storm configuration the named query specified by a tuple field.
     *
     * @param fieldName field's name that contains the named of the query.
     */
    public static final ContextQuery namedByField(final String fieldName) {
        return new ContextQuery.BoundQueryNamedByFieldContext(fieldName);
    }


    /**
     * Maps a CQL value to the specified field from an input tuple.
     *
     * @param name the name of a tuple field.
     * @return a new {@link FieldSelector}.
     */
    public static final FieldSelector field(final String name) {
        return new FieldSelector(name);
    }

    /**
     * Maps CQL values to all specified fields from an input tuple.
     *
     * @param fields a list of tuple fields
     * @return a list of {@link FieldSelector}.
     */
    public static final FieldSelector[] fields(final String... fields) {
        int size = fields.length;
        List<FieldSelector> fl = new ArrayList<>(size);
        for(int i = 0 ; i < size; i++)
                fl.add(new FieldSelector(fields[i]));
        return fl.toArray(new FieldSelector[size]);
    }


    /**
     * Includes all tuple fields.
     */
    public static final CqlMapper.DefaultCqlMapper all() {
        return new CqlMapper.DefaultCqlMapper();
    }
}
