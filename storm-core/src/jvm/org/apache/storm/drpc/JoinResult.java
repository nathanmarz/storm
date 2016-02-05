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
package org.apache.storm.drpc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JoinResult extends BaseRichBolt {
    public static final Logger LOG = LoggerFactory.getLogger(JoinResult.class);

    String returnComponent;
    Map<Object, Tuple> returns = new HashMap<>();
    Map<Object, Tuple> results = new HashMap<>();
    OutputCollector _collector;

    public JoinResult(String returnComponent) {
        this.returnComponent = returnComponent;
    }
 
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    public void execute(Tuple tuple) {
        Object requestId = tuple.getValue(0);
        if(tuple.getSourceComponent().equals(returnComponent)) {
            returns.put(requestId, tuple);
        } else {
            results.put(requestId, tuple);
        }

        if(returns.containsKey(requestId) && results.containsKey(requestId)) {
            Tuple result = results.remove(requestId);
            Tuple returner = returns.remove(requestId);
            LOG.debug(result.getValue(1).toString());
            List<Tuple> anchors = new ArrayList<>();
            anchors.add(result);
            anchors.add(returner);            
            _collector.emit(anchors, new Values(""+result.getValue(1), returner.getValue(1)));
            _collector.ack(result);
            _collector.ack(returner);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("result", "return-info"));
    }
}
