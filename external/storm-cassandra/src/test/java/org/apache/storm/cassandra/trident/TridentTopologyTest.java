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
package org.apache.storm.cassandra.trident;

import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.storm.cassandra.CassandraContext;
import org.apache.storm.cassandra.bolt.BaseTopologyTest;
import org.apache.storm.cassandra.query.CQLStatementTupleMapper;
import org.apache.storm.cassandra.trident.state.CassandraQuery;
import org.apache.storm.cassandra.trident.state.CassandraState;
import org.apache.storm.cassandra.trident.state.CassandraStateFactory;
import org.apache.storm.cassandra.trident.state.CassandraStateUpdater;
import org.apache.storm.cassandra.trident.state.TridentResultSetValuesMapper;
import org.junit.Assert;
import org.junit.Test;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.Random;

import static org.apache.storm.cassandra.DynamicStatementBuilder.boundQuery;
import static org.apache.storm.cassandra.DynamicStatementBuilder.field;
import static org.apache.storm.cassandra.DynamicStatementBuilder.with;

/**
 *
 */
public class TridentTopologyTest extends BaseTopologyTest {

    @Test
    public void testTridentTopology() throws Exception {

        Session session = cassandraCQLUnit.session;
        String[] stationIds = {"station-1", "station-2", "station-3"};
        for (int i = 1; i < 4; i++) {
            ResultSet resultSet = session.execute("INSERT INTO weather.station(id, name) VALUES(?, ?)", stationIds[i-1],
                    "Foo-Station-" + new Random().nextInt());
        }

        ResultSet rows = cassandraCQLUnit.session.execute("SELECT * FROM weather.station");
        for (Row row : rows) {
            System.out.println("####### row = " + row);
        }

        WeatherBatchSpout weatherBatchSpout =
                new WeatherBatchSpout(new Fields("weather_station_id", "temperature", "event_time"), 3,
                        stationIds);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("cassandra-trident-stream", weatherBatchSpout);

        CassandraStateFactory insertValuesStateFactory = getInsertTemperatureStateFactory();

        CassandraStateFactory selectWeatherStationStateFactory = getSelectWeatherStationStateFactory();

        TridentState selectState = topology.newStaticState(selectWeatherStationStateFactory);
        stream = stream.stateQuery(selectState, new Fields("weather_station_id"), new CassandraQuery(), new Fields("name"));
        stream = stream.each(new Fields("name"), new PrintFunction(), new Fields("name_x"));

        stream.partitionPersist(insertValuesStateFactory, new Fields("weather_station_id", "name", "event_time", "temperature"), new CassandraStateUpdater(), new Fields());

        StormTopology stormTopology = topology.build();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordCounter", getConfig(), stormTopology);
        Thread.sleep(30 * 1000);

        rows = cassandraCQLUnit.session.execute("SELECT * FROM weather.temperature");
        Assert.assertTrue(rows.iterator().hasNext()); // basic sanity check

        cluster.killTopology("wordCounter");
        cluster.shutdown();
    }

    public static class PrintFunction extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            System.out.println("####### tuple = " + tuple.getValues());
            collector.emit(tuple.getValues());
        }
    }

    private CassandraStateFactory getInsertTemperatureStateFactory() {
        CassandraState.Options options = new CassandraState.Options(new CassandraContext());
        CQLStatementTupleMapper insertTemperatureValues = boundQuery(
                "INSERT INTO weather.temperature(weather_station_id, weather_station_name, event_time, temperature) VALUES(?, ?, ?, ?)")
                .bind(with(field("weather_station_id"), field("name").as("weather_station_name"), field("event_time").now(), field("temperature")));
        options.withCQLStatementTupleMapper(insertTemperatureValues);
        return new CassandraStateFactory(options);
    }

    public CassandraStateFactory getSelectWeatherStationStateFactory() {
        CassandraState.Options options = new CassandraState.Options(new CassandraContext());
        CQLStatementTupleMapper insertTemperatureValues = boundQuery("SELECT name FROM weather.station WHERE id = ?")
                .bind(with(field("weather_station_id").as("id")));
        options.withCQLStatementTupleMapper(insertTemperatureValues);
        options.withCQLResultSetValuesMapper(new TridentResultSetValuesMapper(new Fields("name")));
        return new CassandraStateFactory(options);
    }
}
