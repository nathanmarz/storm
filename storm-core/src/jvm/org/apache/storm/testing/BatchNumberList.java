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

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBatchBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BatchNumberList extends BaseBatchBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "list"));
    }

    String _wordComponent;
    
    public BatchNumberList(String wordComponent) {
        _wordComponent = wordComponent;
    }
    
    String word = null;
    List<Integer> intSet = new ArrayList<Integer>();
    BatchOutputCollector _collector;
    
    
    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceComponent().equals(_wordComponent)) {
            this.word = tuple.getString(1);
        } else {
            intSet.add(tuple.getInteger(1));
        }
    }

    @Override
    public void finishBatch() {
        if(word!=null) {
            Collections.sort(intSet);
            _collector.emit(new Values(word, intSet));
        }
    }
    
}
