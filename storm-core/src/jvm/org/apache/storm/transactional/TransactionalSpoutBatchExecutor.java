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
package org.apache.storm.transactional;

import org.apache.storm.coordination.BatchOutputCollectorImpl;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import java.math.BigInteger;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalSpoutBatchExecutor implements IRichBolt {
    public static final Logger LOG = LoggerFactory.getLogger(TransactionalSpoutBatchExecutor.class);

    BatchOutputCollectorImpl _collector;
    ITransactionalSpout _spout;
    ITransactionalSpout.Emitter _emitter;
    
    TreeMap<BigInteger, TransactionAttempt> _activeTransactions = new TreeMap<>();

    public TransactionalSpoutBatchExecutor(ITransactionalSpout spout) {
        _spout = spout;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = new BatchOutputCollectorImpl(collector);
        _emitter = _spout.getEmitter(conf, context);
    }

    @Override
    public void execute(Tuple input) {
        TransactionAttempt attempt = (TransactionAttempt) input.getValue(0);
        try {
            if(input.getSourceStreamId().equals(TransactionalSpoutCoordinator.TRANSACTION_COMMIT_STREAM_ID)) {
                if(attempt.equals(_activeTransactions.get(attempt.getTransactionId()))) {
                    ((ICommitterTransactionalSpout.Emitter) _emitter).commit(attempt);
                    _activeTransactions.remove(attempt.getTransactionId());
                    _collector.ack(input);
                } else {
                    _collector.fail(input);
                }
            } else { 
                _emitter.emitBatch(attempt, input.getValue(1), _collector);
                _activeTransactions.put(attempt.getTransactionId(), attempt);
                _collector.ack(input);
                BigInteger committed = (BigInteger) input.getValue(2);
                if(committed!=null) {
                    // valid to delete before what's been committed since 
                    // those batches will never be accessed again
                    _activeTransactions.headMap(committed).clear();
                    _emitter.cleanupBefore(committed);
                }
            }
        } catch(FailedException e) {
            LOG.warn("Failed to emit batch for transaction", e);
            _collector.fail(input);
        }
    }

    @Override
    public void cleanup() {
        _emitter.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _spout.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return _spout.getComponentConfiguration();
    }
}
