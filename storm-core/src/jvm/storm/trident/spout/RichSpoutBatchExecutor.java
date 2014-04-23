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

import backtype.storm.Config;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.RotatingMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import storm.trident.operation.TridentCollector;
import storm.trident.topology.TransactionAttempt;
import storm.trident.util.TridentUtils;

public class RichSpoutBatchExecutor implements ITridentSpout {
    public static final String MAX_BATCH_SIZE_CONF = "topology.spout.max.batch.size";

    IRichSpout _spout;
    
    public RichSpoutBatchExecutor(IRichSpout spout) {
        _spout = spout;
    }

    @Override
    public Map getComponentConfiguration() {
        return _spout.getComponentConfiguration();
    }

    @Override
    public Fields getOutputFields() {
        return TridentUtils.getSingleOutputStreamFields(_spout);
        
    }

    @Override
    public BatchCoordinator getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return new RichSpoutCoordinator();
    }

    @Override
    public Emitter getEmitter(String txStateId, Map conf, TopologyContext context) {
        return new RichSpoutEmitter(conf, context);
    }
    
    class RichSpoutEmitter implements ITridentSpout.Emitter<Object> {
        int _maxBatchSize;
        boolean prepared = false;
        CaptureCollector _collector;
        RotatingMap<Long, List<Object>> idsMap;
        Map _conf;
        TopologyContext _context;
        long lastRotate = System.currentTimeMillis();
        long rotateTime;

        public RichSpoutEmitter(Map conf, TopologyContext context) {
            _conf = conf;
            _context = context;
            Number batchSize = (Number) conf.get(MAX_BATCH_SIZE_CONF);
            if(batchSize==null) batchSize = 1000;
            _maxBatchSize = batchSize.intValue();
            _collector = new CaptureCollector();
            idsMap = new RotatingMap(3);
            rotateTime = 1000L * ((Number)conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
        }
        
        @Override
        public void emitBatch(TransactionAttempt tx, Object coordinatorMeta, TridentCollector collector) {
            long txid = tx.getTransactionId();
            
            long now = System.currentTimeMillis();
            if(now - lastRotate > rotateTime) {
                Map<Long, List<Object>> failed = idsMap.rotate();
                for(Long id: failed.keySet()) {
                    //TODO: this isn't right... it's not in the map anymore
                    fail(id);
                }
                lastRotate = now;
            }
            
            if(idsMap.containsKey(txid)) {
                fail(txid);
            }
            
            _collector.reset(collector);
            if(!prepared) {
                _spout.open(_conf, _context, new SpoutOutputCollector(_collector));
                prepared = true;
            }
            for(int i=0; i<_maxBatchSize; i++) {
                _spout.nextTuple();
                if(_collector.numEmitted < i) {
                    break;
                }
            }
            idsMap.put(txid, _collector.ids);

        }

        @Override
        public void success(TransactionAttempt tx) {
            ack(tx.getTransactionId());
        }

        private void ack(long batchId) {
            List<Object> ids = (List<Object>) idsMap.remove(batchId);
            if(ids!=null) {
                for(Object id: ids) {
                    _spout.ack(id);
                }
            }
        }
        
        private void fail(long batchId) {
            List<Object> ids = (List<Object>) idsMap.remove(batchId);
            if(ids!=null) {
                for(Object id: ids) {
                    _spout.fail(id);
                }
            }
        }
        
        @Override
        public void close() {
        }
        
    }
    
    class RichSpoutCoordinator implements ITridentSpout.BatchCoordinator {
        @Override
        public Object initializeTransaction(long txid, Object prevMetadata, Object currMetadata) {
            return null;
        }

        @Override
        public void success(long txid) {
        }

        @Override
        public boolean isReady(long txid) {
            return true;
        }

        @Override
        public void close() {
        }        
    }
    
    static class CaptureCollector implements ISpoutOutputCollector {

        TridentCollector _collector;
        public List<Object> ids;
        public int numEmitted;
        
        public void reset(TridentCollector c) {
            _collector = c;
            ids = new ArrayList<Object>();
        }
        
        @Override
        public void reportError(Throwable t) {
            _collector.reportError(t);
        }

        @Override
        public List<Integer> emit(String stream, List<Object> values, Object id) {
            if(id!=null) ids.add(id);
            numEmitted++;            
            _collector.emit(values);
            return null;
        }

        @Override
        public void emitDirect(int task, String stream, List<Object> values, Object id) {
            throw new UnsupportedOperationException("Trident does not support direct streams");
        }
        
    }
    
}
