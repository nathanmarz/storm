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
package org.apache.storm.coordination;

import org.apache.storm.coordination.CoordinatedBolt.FinishedCallback;
import org.apache.storm.coordination.CoordinatedBolt.TimeoutCallback;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchBoltExecutor implements IRichBolt, FinishedCallback, TimeoutCallback {
    public static final Logger LOG = LoggerFactory.getLogger(BatchBoltExecutor.class);

    byte[] _boltSer;
    Map<Object, IBatchBolt> _openTransactions;
    Map _conf;
    TopologyContext _context;
    BatchOutputCollectorImpl _collector;
    
    public BatchBoltExecutor(IBatchBolt bolt) {
        _boltSer = Utils.javaSerialize(bolt);
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _conf = conf;
        _context = context;
        _collector = new BatchOutputCollectorImpl(collector);
        _openTransactions = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        Object id = input.getValue(0);
        IBatchBolt bolt = getBatchBolt(id);
        try {
             bolt.execute(input);
            _collector.ack(input);
        } catch(FailedException e) {
            LOG.error("Failed to process tuple in batch", e);
            _collector.fail(input);                
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void finishedId(Object id) {
        IBatchBolt bolt = getBatchBolt(id);
        _openTransactions.remove(id);
        bolt.finishBatch();
    }

    @Override
    public void timeoutId(Object attempt) {
        _openTransactions.remove(attempt);        
    }    
    

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        newTransactionalBolt().declareOutputFields(declarer);
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return newTransactionalBolt().getComponentConfiguration();
    }
    
    private IBatchBolt getBatchBolt(Object id) {
        IBatchBolt bolt = _openTransactions.get(id);
        if(bolt==null) {
            bolt = newTransactionalBolt();
            bolt.prepare(_conf, _context, _collector, id);
            _openTransactions.put(id, bolt);            
        }
        return bolt;
    }
    
    private IBatchBolt newTransactionalBolt() {
        return Utils.javaDeserialize(_boltSer, IBatchBolt.class);
    }
}
