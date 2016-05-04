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
package org.apache.storm.testing;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.storm.utils.Utils.tuple;


public class TestAggregatesCounter extends BaseRichBolt {
    public static Logger LOG = LoggerFactory.getLogger(TestWordCounter.class);

    Map<String, Integer> _counts;
    OutputCollector _collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _counts = new HashMap<String, Integer>();
    }

    public void execute(Tuple input) {
        String word = (String) input.getValues().get(0);
        int count = (Integer) input.getValues().get(1);
        _counts.put(word, count);
        int globalCount = 0;
        for(String w: _counts.keySet()) {
            globalCount+=_counts.get(w);
        }
        _collector.emit(tuple(globalCount));
        _collector.ack(input);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("agg-global"));
    }
}
