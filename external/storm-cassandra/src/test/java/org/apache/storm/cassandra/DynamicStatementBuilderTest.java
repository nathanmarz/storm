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
import com.datastax.driver.core.Statement;
import com.google.common.collect.Maps;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static org.apache.storm.cassandra.DynamicStatementBuilder.*;
import static org.mockito.Mockito.when;

public class DynamicStatementBuilderTest {

    private static final Date NOW = new Date();

    private static final Tuple mockTuple;

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("schema.cql","weather"));

    static {
        mockTuple = Mockito.mock(Tuple.class);
        when(mockTuple.getValueByField("weatherstation_id")).thenReturn("1");
        when(mockTuple.getValueByField("event_time")).thenReturn(NOW);
        when(mockTuple.getValueByField("temperature")).thenReturn("0°C");
        when(mockTuple.getFields()).thenReturn(new Fields("weatherstation_id", "event_time", "temperature"));
    }

    @Test
    public void shouldBuildMultipleStaticInsertStatementGivenKeyspaceAndAllMapper() {
        executeStatementAndAssert(
                async(
                        insertInto("weather", "temperature").values(all()),
                        insertInto("weather", "temperature").values(all())
                ),
                "INSERT INTO weather.temperature(weatherstation_id,event_time,temperature) VALUES ('1'," + NOW.getTime() + ",'0°C');",
                "INSERT INTO weather.temperature(weatherstation_id,event_time,temperature) VALUES ('1'," + NOW.getTime() + ",'0°C');"
        );
    }

    @Test
    public void shouldBuildStaticInsertStatementGivenKeyspaceAndAllMapper() {
        executeStatementAndAssert(insertInto("weather", "temperature").values(all()).build(),
                "INSERT INTO weather.temperature(weatherstation_id,event_time,temperature) VALUES ('1'," + NOW.getTime() + ",'0°C');");
    }

    @Test
    public void shouldBuildStaticInsertStatementGivenNoKeyspaceAllMapper() {
        executeStatementAndAssert(insertInto("temperature").values(all()).build(),
                "INSERT INTO temperature(weatherstation_id,event_time,temperature) VALUES ('1'," + NOW.getTime() + ",'0°C');");
    }

    @Test
    public void shouldBuildStaticInsertStatementGivenNoKeyspaceAndWithFieldsMapper() {
        executeStatementAndAssert(insertInto("temperature").values(with(fields("weatherstation_id", "event_time", "temperature"))).build(),
                "INSERT INTO temperature(weatherstation_id,event_time,temperature) VALUES ('1'," + NOW.getTime() + ",'0°C');");
    }

    @Test
    public void shouldBuildStaticLoggedBatchStatementGivenNoKeyspaceAndWithFieldsMapper() {
        executeBatchStatementAndAssert(loggedBatch(
                insertInto("temperature").values(with(fields("weatherstation_id", "event_time", "temperature")))
        ), "INSERT INTO temperature(weatherstation_id,event_time,temperature) VALUES ('1'," + NOW.getTime() + ",'0°C');");
    }

    @Test
    public void shouldBuildStaticUnloggedBatchStatementGivenNoKeyspaceAndWithFieldsMapper() {
        executeBatchStatementAndAssert(unLoggedBatch(
                insertInto("temperature").values(with(fields("weatherstation_id", "event_time", "temperature")))
        ),  "INSERT INTO temperature(weatherstation_id,event_time,temperature) VALUES ('1'," + NOW.getTime() + ",'0°C');");
    }

    private void executeBatchStatementAndAssert(CQLStatementTupleMapper mapper, String... results) {
        List<Statement> map = mapper.map(Maps.newHashMap(), cassandraCQLUnit.session, mockTuple);

        BatchStatement statement = (BatchStatement)map.get(0);
        Collection<Statement> statements = statement.getStatements();
        Assert.assertEquals(results.length, statements.size());

        for(Statement s : statements)
            Assert.assertTrue(Arrays.asList(results).contains(s.toString()));
    }


    private void executeStatementAndAssert(CQLStatementTupleMapper mapper, String... expected) {
        List<Statement> map = mapper.map(Maps.newHashMap(), cassandraCQLUnit.session, mockTuple);

        List<String> listExpected = Arrays.asList(expected);
        for( int i=0; i< map.size(); i++) {
            Assert.assertEquals(listExpected.get(i), map.get(i).toString());
        }
    }

    @Test
    public void shouldBuildStaticBoundStatement() {
        CQLStatementTupleMapper mapper = boundQuery("INSERT INTO weather.temperature(weatherstation_id, event_time, temperature) VALUES(?, ?, ?)")
                .bind(with(field("weatherstation_id"), field("event_time").now(), field("temperature")));
        List<Statement> map = mapper.map(Maps.newHashMap(), cassandraCQLUnit.session, mockTuple);
        Statement statement = map.get(0);
        Assert.assertNotNull(statement);
    }

}