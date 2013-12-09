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
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;

public class CountingCommitBolt extends BaseTransactionalBolt implements ICommitter {
    BatchOutputCollector _collector;
    TransactionAttempt _id;
    int _count = 0;
    
    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
        _id = id;
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        _count++;
    }

    @Override
    public void finishBatch() {
        _collector.emit(new Values(_id, _count));        
    }   

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tx", "count"));
    }

}
