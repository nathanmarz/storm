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

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class BatchRepeatA extends BaseBasicBolt {  
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
       Object id = input.getValue(0);
       String word = input.getString(1);
       for(int i=0; i<word.length(); i++) {
            if(word.charAt(i) == 'a') {
                collector.emit("multi", new Values(id, word.substring(0, i)));
            }
        }
        collector.emit("single", new Values(id, word));
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("multi", new Fields("id", "word"));
        declarer.declareStream("single", new Fields("id", "word"));
    }

}
