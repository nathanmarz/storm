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

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Maps;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.apache.storm.cassandra.query.impl.BatchCQLStatementTupleMapper;
import org.apache.storm.cassandra.query.impl.SimpleCQLStatementMapper;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import static org.apache.storm.cassandra.DynamicStatementBuilder.simpleQuery;
import static org.apache.storm.cassandra.DynamicStatementBuilder.all;
import static org.apache.storm.cassandra.DynamicStatementBuilder.async;
import static org.apache.storm.cassandra.DynamicStatementBuilder.boundQuery;
import static org.apache.storm.cassandra.DynamicStatementBuilder.field;
import static org.apache.storm.cassandra.DynamicStatementBuilder.fields;
import static org.apache.storm.cassandra.DynamicStatementBuilder.loggedBatch;
import static org.apache.storm.cassandra.DynamicStatementBuilder.unLoggedBatch;
import static org.mockito.Mockito.when;

public class DynamicStatementBuilderTest {

    private static final Date NOW = new Date();

    private static final Tuple mockTuple;

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("schema.cql", "weather"));

    static {
        mockTuple = Mockito.mock(Tuple.class);
        when(mockTuple.getValueByField("weather_station_id")).thenReturn("1");
        when(mockTuple.getValueByField("event_time")).thenReturn(NOW);
        when(mockTuple.getValueByField("temperature")).thenReturn("0°C");
        when(mockTuple.getFields()).thenReturn(new Fields("weather_station_id", "event_time", "temperature"));
    }

    public static final String QUERY_STRING = "INSERT INTO weather.temperature(weather_station_id,event_time,temperature) VALUES (?,?,?);";

    @Test
    public void shouldBuildMultipleStaticInsertStatementGivenKeyspaceAndAllMapper() {
        CQLStatementTupleMapper mapper = async(
                simpleQuery(QUERY_STRING).with(all()),
                simpleQuery(QUERY_STRING).with(all())
        );
        List<Statement> stmts = mapper.map(Maps.newHashMap(), cassandraCQLUnit.session, mockTuple);
        Assert.assertEquals(2, stmts.size());
        makeAssertSimpleStatement(QUERY_STRING, stmts.get(0));
        makeAssertSimpleStatement(QUERY_STRING, stmts.get(1));
    }

    @Test
    public void shouldBuildStaticInsertStatementGivenKeyspaceAndAllMapper() {
        SimpleCQLStatementMapper mapper = simpleQuery(QUERY_STRING).with(all()).build();
        List<Statement> stmts = mapper.map(Maps.newHashMap(), cassandraCQLUnit.session, mockTuple);
        Assert.assertEquals(1, stmts.size());
        makeAssertSimpleStatement(QUERY_STRING, stmts.get(0));
    }

    @Test
    public void shouldBuildStaticInsertStatementUsingBuilderGivenKeyspaceAndAllMapper() {
        Insert insert = QueryBuilder.insertInto("weather", "temperature")
                .value("weather_station_id", "?")
                .value("event_time", "?")
                .value("temperature", "?");
        SimpleCQLStatementMapper mapper = simpleQuery(insert).with(all()).build();
        List<Statement> stmts = mapper.map(Maps.newHashMap(), cassandraCQLUnit.session, mockTuple);
        Assert.assertEquals(1, stmts.size());
        makeAssertSimpleStatement(QUERY_STRING, stmts.get(0));
    }

    @Test
    public void shouldBuildStaticInsertStatementGivenNoKeyspaceAllMapper() {
        SimpleCQLStatementMapper mapper = simpleQuery(QUERY_STRING).with(all()).build();
        List<Statement> stmts = mapper.map(Maps.newHashMap(), cassandraCQLUnit.session, mockTuple);
        Assert.assertEquals(1, stmts.size());
        makeAssertSimpleStatement(QUERY_STRING, stmts.get(0));
    }

    @Test
    public void shouldBuildStaticInsertStatementGivenNoKeyspaceAndWithFieldsMapper() {
        SimpleCQLStatementMapper mapper = simpleQuery(QUERY_STRING).with(fields("weather_station_id", "event_time", "temperature")).build();
        List<Statement> stmts = mapper.map(Maps.newHashMap(), cassandraCQLUnit.session, mockTuple);
        Assert.assertEquals(1, stmts.size());
        makeAssertSimpleStatement(QUERY_STRING, stmts.get(0));
    }

    @Test
    public void shouldBuildStaticLoggedBatchStatementGivenNoKeyspaceAndWithFieldsMapper() {
        BatchCQLStatementTupleMapper mapper = loggedBatch(simpleQuery(QUERY_STRING).with(fields("weather_station_id", "event_time", "temperature")));
        List<Statement> stmts = mapper.map(Maps.newHashMap(), cassandraCQLUnit.session, mockTuple);
        Assert.assertEquals(1, stmts.size());
        makeAssertBatchStatement(QUERY_STRING, stmts.get(0));
    }

    @Test
    public void shouldBuildStaticUnloggedBatchStatementGivenNoKeyspaceAndWithFieldsMapper() {
        BatchCQLStatementTupleMapper mapper = unLoggedBatch(simpleQuery(QUERY_STRING).with(fields("weather_station_id", "event_time", "temperature")));
        List<Statement> stmts = mapper.map(Maps.newHashMap(), cassandraCQLUnit.session, mockTuple);
        Assert.assertEquals(1, stmts.size());
        makeAssertBatchStatement(QUERY_STRING, stmts.get(0));
    }

    @Test
    public void shouldBuildStaticBoundStatement() {
        CQLStatementTupleMapper mapper = async(boundQuery(QUERY_STRING).bind(field("weather_station_id"), field("event_time").now(), field("temperature")));
        List<Statement> map = mapper.map(Maps.newHashMap(), cassandraCQLUnit.session, mockTuple);
        Statement statement = map.get(0);
        Assert.assertNotNull(statement);
    }

    public void makeAssertSimpleStatement(String expected, Statement actual) {
        Assert.assertTrue(actual instanceof SimpleStatement);
        Assert.assertEquals(expected, actual.toString());
        Assert.assertEquals(3, ((SimpleStatement)actual).getValues(ProtocolVersion.V3).length);
    }

    public void makeAssertBatchStatement(String expected, Statement actual) {
        Assert.assertTrue(actual instanceof BatchStatement);
        Collection<Statement> statements = ((BatchStatement) actual).getStatements();
        for(Statement s : statements) {
            Assert.assertEquals(expected, s.toString());
            Assert.assertEquals(3, ((SimpleStatement)s).getValues(ProtocolVersion.V3).length);
        }
    }
}