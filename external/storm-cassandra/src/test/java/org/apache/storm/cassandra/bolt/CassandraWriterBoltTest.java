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
package org.apache.storm.cassandra.bolt;

import backtype.storm.topology.TopologyBuilder;
import com.datastax.driver.core.ResultSet;
import org.apache.storm.cassandra.WeatherSpout;
import org.apache.storm.cassandra.query.SimpleCQLStatementTupleMapper;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.storm.cassandra.DynamicStatementBuilder.field;
import static org.apache.storm.cassandra.DynamicStatementBuilder.insertInto;
import static org.apache.storm.cassandra.DynamicStatementBuilder.with;

/**
 *
 */
public class CassandraWriterBoltTest extends BaseTopologyTest {

    public static final String SPOUT_MOCK = "spout-mock";
    public static final String BOLT_WRITER = "writer";

    @Test
    @Ignore("The sleep method should be used in tests")
    public void shouldAsyncInsertGivenStaticTableNameAndDynamicQueryBuildFromAllTupleFields() {
        executeAndAssertWith(100000, new CassandraWriterBolt((getInsertInto())));
    }

    private SimpleCQLStatementTupleMapper getInsertInto() {
        return insertInto("weather", "temperature").values(with(field("weather_station_id"), field("event_time").now(), field("temperature"))).build();
    }

    protected void executeAndAssertWith(final int maxQueries, final BaseCassandraBolt bolt) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_MOCK, new WeatherSpout("test", maxQueries), 1)
                .setMaxTaskParallelism(1);

        builder.setBolt(BOLT_WRITER, bolt, 4)
                .shuffleGrouping(SPOUT_MOCK);

        runLocalTopologyAndWait(builder);

        ResultSet rows = cassandraCQLUnit.session.execute("SELECT * FROM weather.temperature WHERE weather_station_id='test'");
        Assert.assertEquals(maxQueries, rows.all().size());
    }


}
