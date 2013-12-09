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

import backtype.storm.Config;
import backtype.storm.topology.OutputFieldsDeclarer;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import java.util.HashMap;


public class TestPlannerSpout extends BaseRichSpout {
    boolean _isDistributed;
    Fields _outFields;
    
    public TestPlannerSpout(Fields outFields, boolean isDistributed) {
        _isDistributed = isDistributed;
        _outFields = outFields;
    }

    public TestPlannerSpout(boolean isDistributed) {
        this(new Fields("field1", "field2"), isDistributed);
    }
        
    public TestPlannerSpout(Fields outFields) {
        this(outFields, true);
    }
    
    public Fields getOutputFields() {
        return _outFields;
    }

    
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        
    }
    
    public void close() {
        
    }
    
    public void nextTuple() {
        Utils.sleep(100);
    }
    
    public void ack(Object msgId){
        
    }

    public void fail(Object msgId){
        
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(getOutputFields());
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> ret = new HashMap<String, Object>();
        if(!_isDistributed) {
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        }
        return ret;
    }       
}