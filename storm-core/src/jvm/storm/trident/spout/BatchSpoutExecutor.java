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

import backtype.storm.task.TopologyContext;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.tuple.Fields;
import java.util.Map;
import storm.trident.operation.TridentCollector;

public class BatchSpoutExecutor implements ITridentSpout {
    public static class EmptyCoordinator implements BatchCoordinator {
        @Override
        public Object initializeTransaction(long txid, Object prevMetadata, Object currMetadata) {
            return null;
        }

        @Override
        public void close() {
        }

        @Override
        public void success(long txid) {
        }

        @Override
        public boolean isReady(long txid) {
            return true;
        }
    }
    
    public class BatchSpoutEmitter implements Emitter {

        @Override
        public void emitBatch(TransactionAttempt tx, Object coordinatorMeta, TridentCollector collector) {
            _spout.emitBatch(tx.getTransactionId(), collector);
        }        
        
        @Override
        public void success(TransactionAttempt tx) {
            _spout.ack(tx.getTransactionId());
        }

        @Override
        public void close() {
            _spout.close();
        }        
    }
    
    IBatchSpout _spout;
    
    public BatchSpoutExecutor(IBatchSpout spout) {
        _spout = spout;
    }
    
    @Override
    public BatchCoordinator getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return new EmptyCoordinator();
    }

    @Override
    public Emitter getEmitter(String txStateId, Map conf, TopologyContext context) {
        _spout.open(conf, context);
        return new BatchSpoutEmitter();
    }

    @Override
    public Map getComponentConfiguration() {
        return _spout.getComponentConfiguration();
    }

    @Override
    public Fields getOutputFields() {
        return _spout.getOutputFields();
    }
    
}
