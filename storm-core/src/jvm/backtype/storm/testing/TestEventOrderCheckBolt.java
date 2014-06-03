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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.testing;

import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TestEventOrderCheckBolt extends BaseRichBolt {
    public static Logger LOG = LoggerFactory.getLogger(TestEventOrderCheckBolt.class);
    
    private int _count;
    OutputCollector _collector;
    Map<Integer, Long> recentEventId = new HashMap<Integer, Long>();

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _count = 0;
    }

    public void execute(Tuple input) {
        Integer sourceId = input.getInteger(0);
        Long eventId = input.getLong(1);
        Long recentEvent = recentEventId.get(sourceId);

        if (null != recentEvent && eventId <= recentEvent) {
            String error = "Error: event id is not in strict order! event source Id: "
                    + sourceId + ", last event Id: " + recentEvent + ", current event Id: " + eventId;

            _collector.emit(input, new Values(error));
        }
        recentEventId.put(sourceId, eventId);

        _collector.ack(input);
    }

    public void cleanup() {

    }

    public Fields getOutputFields() {
        return new Fields("error");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("error"));
    }
}