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

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;


public class TupleCaptureBolt implements IRichBolt {
    public static final transient Map<String, Map<String, List<FixedTuple>>> emitted_tuples = new HashMap<>();

    private String _name;
    private OutputCollector _collector;

    public TupleCaptureBolt() {
        _name = UUID.randomUUID().toString();
        emitted_tuples.put(_name, new HashMap<String, List<FixedTuple>>());
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    public void execute(Tuple input) {
        String component = input.getSourceComponent();
        Map<String, List<FixedTuple>> captured = emitted_tuples.get(_name);
        if(!captured.containsKey(component)) {
           captured.put(component, new ArrayList<FixedTuple>());
        }
        captured.get(component).add(new FixedTuple(input.getSourceStreamId(), input.getValues()));
        _collector.ack(input);
    }

    public Map<String, List<FixedTuple>> getResults() {
        return emitted_tuples.get(_name);
    }

    public void cleanup() {
    }
    
    public Map<String, List<FixedTuple>> getAndRemoveResults() {
        return emitted_tuples.remove(_name);
    }

    public Map<String, List<FixedTuple>> getAndClearResults() {
        Map<String, List<FixedTuple>> ret = new HashMap<>(emitted_tuples.get(_name));
        emitted_tuples.get(_name).clear();
        return ret;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
