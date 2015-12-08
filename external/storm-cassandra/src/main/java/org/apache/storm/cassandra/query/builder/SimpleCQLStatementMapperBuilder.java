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

import com.datastax.driver.core.querybuilder.BuiltStatement;
import org.apache.storm.cassandra.query.CQLStatementBuilder;
import org.apache.storm.cassandra.query.CqlMapper;
import org.apache.storm.cassandra.query.impl.RoutingKeyGenerator;
import org.apache.storm.cassandra.query.impl.SimpleCQLStatementMapper;
import org.apache.storm.cassandra.query.selector.FieldSelector;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Default class to build {@link org.apache.storm.cassandra.query.impl.SimpleCQLStatementMapper} instance.
 */
public class SimpleCQLStatementMapperBuilder implements CQLStatementBuilder<SimpleCQLStatementMapper>, Serializable {

    private final String queryString;

    private CqlMapper mapper;

    private List<String> routingKeys;

    /**
     * Creates a new {@link SimpleCQLStatementMapperBuilder} instance.
     * @param queryString a valid CQL query string.
     */
    public SimpleCQLStatementMapperBuilder(String queryString) {
        this.queryString = queryString;
    }

    /**
     * Creates a new {@link SimpleCQLStatementMapperBuilder} instance.
     * @param builtStatement a query built statements
     */
    public SimpleCQLStatementMapperBuilder(BuiltStatement builtStatement) {
        this.queryString = builtStatement.getQueryString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SimpleCQLStatementMapper build() {
        return new SimpleCQLStatementMapper(queryString, mapper, (routingKeys != null) ? new RoutingKeyGenerator(routingKeys) : null );
    }

    /**
     * Includes only the specified tuple fields.
     *
     * @param fields a list of field selector.
     */
    public final SimpleCQLStatementMapperBuilder with(final FieldSelector... fields) {
        this.mapper = new CqlMapper.SelectableCqlMapper(Arrays.asList(fields));
        return this;
    }

    public final SimpleCQLStatementMapperBuilder withRoutingKeys(String...fields) {
        this.routingKeys = Arrays.asList(fields);
        return this;
    }

    /**
     * Includes only the specified tuple fields.
     *
     * @param mapper a column mapper.
     */
    public final SimpleCQLStatementMapperBuilder with(CqlMapper mapper) {
        this.mapper = mapper;
        return this;
    }
}

