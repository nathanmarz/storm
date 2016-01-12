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

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Assert;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class WeatherSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;

    private String stationID;

    private AtomicLong maxQueries;

    private AtomicLong acks = new AtomicLong(0);

    private AtomicLong emit = new AtomicLong(0);

    /**
     * Creates a new {@link WeatherSpout} instance.
     * @param stationID The station ID.
     */
    public WeatherSpout(String stationID, int maxQueries) {
        this.stationID = stationID;
        this.maxQueries = new AtomicLong(maxQueries);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("weather_station_id", "temperature"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void ack(Object msgId) {
        acks.incrementAndGet();
    }

    @Override
    public void fail(Object msgId) {
        Assert.fail("Must never get fail tuple : " + msgId);
    }

    @Override
    public void close() {
        Assert.assertEquals(acks.get(), emit.get());
    }

    @Override
    public void nextTuple() {
        if (emit.get() < maxQueries.get()) {
            spoutOutputCollector.emit(new Values(stationID, "38°C"), emit.incrementAndGet());
        }
    }
}
