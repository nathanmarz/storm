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
package org.apache.storm.trident.testing;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import java.util.List;
import java.util.Map;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ICommitterTridentSpout;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;


public class FeederCommitterBatchSpout implements ICommitterTridentSpout, IFeeder {

    FeederBatchSpout _spout;
    
    public FeederCommitterBatchSpout(List<String> fields) {
        _spout = new FeederBatchSpout(fields);
    }
    
    public void setWaitToEmit(boolean trueIfWait) {
        _spout.setWaitToEmit(trueIfWait);
    }
    
    static class CommitterEmitter implements ICommitterTridentSpout.Emitter {
        ITridentSpout.Emitter _emitter;
        
        
        public CommitterEmitter(ITridentSpout.Emitter e) {
            _emitter = e;
        }
        
        @Override
        public void commit(TransactionAttempt attempt) {
        }

        @Override
        public void emitBatch(TransactionAttempt tx, Object coordinatorMeta, TridentCollector collector) {
            _emitter.emitBatch(tx, coordinatorMeta, collector);
        }

        @Override
        public void success(TransactionAttempt tx) {
            _emitter.success(tx);
        }

        @Override
        public void close() {
            _emitter.close();
        }
        
    }
    
    @Override
    public Emitter getEmitter(String txStateId, Map conf, TopologyContext context) {
        return new CommitterEmitter(_spout.getEmitter(txStateId, conf, context));
    }

    @Override
    public BatchCoordinator getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return _spout.getCoordinator(txStateId, conf, context);
    }

    @Override
    public Fields getOutputFields() {
        return _spout.getOutputFields();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return _spout.getComponentConfiguration();
    }

    @Override
    public void feed(Object tuples) {
        _spout.feed(tuples);
    }
    
}
