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
package org.apache.storm.cassandra.query.builder;

import org.apache.storm.cassandra.query.CQLStatementBuilder;
import org.apache.storm.cassandra.query.ContextQuery;
import org.apache.storm.cassandra.query.CqlMapper;
import org.apache.storm.cassandra.query.impl.BoundCQLStatementTupleMapper;
import org.apache.storm.cassandra.query.impl.PreparedStatementBinder;
import org.apache.storm.cassandra.query.impl.RoutingKeyGenerator;
import org.apache.storm.cassandra.query.selector.FieldSelector;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.storm.cassandra.query.ContextQuery.*;

public class BoundCQLStatementMapperBuilder implements CQLStatementBuilder<BoundCQLStatementTupleMapper>,  Serializable {

    private final ContextQuery contextQuery;

    private CqlMapper mapper;

    private List<String> routingKeys;

    private PreparedStatementBinder binder;

    /**
     * Creates a new {@link BoundCQLStatementMapperBuilder} instance.
     * @param cql
     */
    public BoundCQLStatementMapperBuilder(String cql) {
        this.contextQuery = new StaticContextQuery(cql);
    }

    /**
     * Creates a new {@link BoundCQLStatementMapperBuilder} instance.
     * @param contextQuery
     */
    public BoundCQLStatementMapperBuilder(ContextQuery contextQuery) {
        this.contextQuery = contextQuery;
    }

    /**
     * Includes only the specified tuple fields.
     *
     * @param fields a list of field selector.
     */
    public final BoundCQLStatementMapperBuilder bind(final FieldSelector... fields) {
        this.mapper = new CqlMapper.SelectableCqlMapper(Arrays.asList(fields));
        return this;
    }

    /**
     * Includes only the specified tuple fields.
     *
     * @param mapper a column mapper.
     */
    public final BoundCQLStatementMapperBuilder bind(CqlMapper mapper) {
        this.mapper = mapper;
        return this;
    }

    public final BoundCQLStatementMapperBuilder withRoutingKeys(String...fields) {
        this.routingKeys = Arrays.asList(fields);
        return this;
    }

    public final BoundCQLStatementMapperBuilder byNamedSetters() {
        this.binder = new PreparedStatementBinder.CQL3NamedSettersBinder();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundCQLStatementTupleMapper build() {
        return new BoundCQLStatementTupleMapper(contextQuery, mapper, getRoutingKeyGenerator(), getStatementBinder());
    }

    private RoutingKeyGenerator getRoutingKeyGenerator() {
        return (routingKeys != null) ? new RoutingKeyGenerator(routingKeys) : null;
    }

    private PreparedStatementBinder getStatementBinder() {
        return (binder != null) ? binder : new PreparedStatementBinder.DefaultBinder();
    }
}
