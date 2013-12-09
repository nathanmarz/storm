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
package storm.trident.spout;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.topology.BatchInfo;
import storm.trident.topology.ITridentBatchBolt;
import storm.trident.topology.MasterBatchCoordinator;
import storm.trident.tuple.ConsList;

public class TridentSpoutExecutor implements ITridentBatchBolt {
    public static String ID_FIELD = "$tx";
    
    public static Logger LOG = LoggerFactory.getLogger(TridentSpoutExecutor.class);    

    AddIdCollector _collector;
    ITridentSpout _spout;
    ITridentSpout.Emitter _emitter;
    String _streamName;
    String _txStateId;
    
    TreeMap<Long, TransactionAttempt> _activeBatches = new TreeMap<Long, TransactionAttempt>();

    public TridentSpoutExecutor(String txStateId, String streamName, ITridentSpout spout) {
        _txStateId = txStateId;
        _spout = spout;
        _streamName = streamName;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector) {
        _emitter = _spout.getEmitter(_txStateId, conf, context);
        _collector = new AddIdCollector(_streamName, collector);
    }

    @Override
    public void execute(BatchInfo info, Tuple input) {
        // there won't be a BatchInfo for the success stream
        TransactionAttempt attempt = (TransactionAttempt) input.getValue(0);
        if(input.getSourceStreamId().equals(MasterBatchCoordinator.COMMIT_STREAM_ID)) {
            if(attempt.equals(_activeBatches.get(attempt.getTransactionId()))) {
                ((ICommitterTridentSpout.Emitter) _emitter).commit(attempt);
                _activeBatches.remove(attempt.getTransactionId());
            } else {
                 throw new FailedException("Received commit for different transaction attempt");
            }
        } else if(input.getSourceStreamId().equals(MasterBatchCoordinator.SUCCESS_STREAM_ID)) {
            // valid to delete before what's been committed since 
            // those batches will never be accessed again
            _activeBatches.headMap(attempt.getTransactionId()).clear();
            _emitter.success(attempt);
        } else {            
            _collector.setBatch(info.batchId);
            _emitter.emitBatch(attempt, input.getValue(1), _collector);
            _activeBatches.put(attempt.getTransactionId(), attempt);
        }
    }

    @Override
    public void cleanup() {
        _emitter.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> fields = new ArrayList(_spout.getOutputFields().toList());
        fields.add(0, ID_FIELD);
        declarer.declareStream(_streamName, new Fields(fields));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return _spout.getComponentConfiguration();
    }

    @Override
    public void finishBatch(BatchInfo batchInfo) {
    }

    @Override
    public Object initBatchState(String batchGroup, Object batchId) {
        return null;
    }
    
    private static class AddIdCollector implements TridentCollector {
        BatchOutputCollector _delegate;
        Object _id;
        String _stream;
        
        public AddIdCollector(String stream, BatchOutputCollector c) {
            _delegate = c;
            _stream = stream;
        }
        
        
        public void setBatch(Object id) {
            _id = id;
        }        

        @Override
        public void emit(List<Object> values) {
            _delegate.emit(_stream, new ConsList(_id, values));
        }

        @Override
        public void reportError(Throwable t) {
            _delegate.reportError(t);
        }        
    }
}
