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

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;


public class TestConfBolt extends BaseBasicBolt {
    Map<String, Object> _componentConf;
    Map<String, Object> _conf;

    public TestConfBolt() {
        this(null);
    }
        
    public TestConfBolt(Map<String, Object> componentConf) {
        _componentConf = componentConf;
    }        

    @Override
    public void prepare(Map conf, TopologyContext context) {
        _conf = conf;
    }    
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("conf", "value"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String name = input.getString(0);
        collector.emit(new Values(name, _conf.get(name)));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return _componentConf;
    }    
}
