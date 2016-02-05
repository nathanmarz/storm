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

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.RegisteredGlobalState;
import org.apache.storm.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This spout only works in local mode.
 */
public class OpaqueMemoryTransactionalSpout implements IOpaquePartitionedTransactionalSpout<MemoryTransactionalSpoutMeta> {
    public static final String TX_FIELD = MemoryTransactionalSpout.class.getName() + "/id";
    
    private String _id;
    private String _finishedPartitionsId;
    private String _disabledId;
    private int _takeAmt;
    private Fields _outFields;
    
    public OpaqueMemoryTransactionalSpout(Map<Integer, List<List<Object>>> partitions, Fields outFields, int takeAmt) {
        _id = RegisteredGlobalState.registerState(partitions);
        
        Map<Integer, Boolean> finished = Collections.synchronizedMap(new HashMap<Integer, Boolean>());
        _finishedPartitionsId = RegisteredGlobalState.registerState(finished);
        
        Map<Integer, Boolean> disabled = Collections.synchronizedMap(new HashMap<Integer, Boolean>());
        _disabledId = RegisteredGlobalState.registerState(disabled);
        
        _takeAmt = takeAmt;
        _outFields = outFields;
    }
    
    public void setDisabled(Integer partition, boolean disabled) {
        getDisabledStatuses().put(partition, disabled);
    }
    
    public boolean isExhaustedTuples() {
        Map<Integer, Boolean> statuses = getFinishedStatuses();
        for(Integer partition: getQueues().keySet()) {
            if(!statuses.containsKey(partition) || !getFinishedStatuses().get(partition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public IOpaquePartitionedTransactionalSpout.Emitter<MemoryTransactionalSpoutMeta> getEmitter(Map conf, TopologyContext context) {
        return new Emitter(conf);
    }

    @Override
    public IOpaquePartitionedTransactionalSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new Coordinator();
    }
    
    private static class Coordinator implements IOpaquePartitionedTransactionalSpout.Coordinator {
        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void close() {
        }
    }
    
    class Emitter implements IOpaquePartitionedTransactionalSpout.Emitter<MemoryTransactionalSpoutMeta> {
        
        Integer _maxSpoutPending;
        Map<Integer, Integer> _emptyPartitions = new HashMap<>();
        
        public Emitter(Map conf) {
            Object c = conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING);
            if(c==null) _maxSpoutPending = 1;
            else _maxSpoutPending = Utils.getInt(c);
        }
        
        
        @Override
        public MemoryTransactionalSpoutMeta emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition, MemoryTransactionalSpoutMeta lastPartitionMeta) {
            if(!Boolean.FALSE.equals(getDisabledStatuses().get(partition))) {
                int index;
                if(lastPartitionMeta==null) {
                    index = 0;
                } else {
                    index = lastPartitionMeta.index + lastPartitionMeta.amt;
                }
                List<List<Object>> queue = getQueues().get(partition);
                int total = queue.size();
                int left = total - index;
                int toTake = Math.min(left, _takeAmt);

                MemoryTransactionalSpoutMeta ret = new MemoryTransactionalSpoutMeta(index, toTake);
                for(int i=ret.index; i < ret.index + ret.amt; i++) {
                    List<Object> toEmit = new ArrayList<>(queue.get(i));
                    toEmit.add(0, tx);
                    collector.emit(toEmit);                
                }
                if(toTake==0) {
                    // this is a pretty hacky way to determine when all the partitions have been committed
                    // wait until we've emitted max-spout-pending empty partitions for the partition
                    int curr = Utils.get(_emptyPartitions, partition, 0) + 1;
                    _emptyPartitions.put(partition, curr);
                    if(curr > _maxSpoutPending) {
                        getFinishedStatuses().put(partition, true);
                    }
                }
                return ret; 
            } else {
                return null;
            }
        }
                
        @Override
        public void close() {
        }

        @Override
        public int numPartitions() {
            return getQueues().size();
        }
    } 

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> toDeclare = new ArrayList<>(_outFields.toList());
        toDeclare.add(0, TX_FIELD);
        declarer.declare(new Fields(toDeclare));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.registerSerialization(MemoryTransactionalSpoutMeta.class);
        return conf;
    }
    
    public void startup() {
        getFinishedStatuses().clear();
    }
    
    public void cleanup() {
        RegisteredGlobalState.clearState(_id);
        RegisteredGlobalState.clearState(_finishedPartitionsId);
    }
    
    private Map<Integer, List<List<Object>>> getQueues() {
        return (Map<Integer, List<List<Object>>>) RegisteredGlobalState.getState(_id);
    }
    
    private Map<Integer, Boolean> getFinishedStatuses() {
        return (Map<Integer, Boolean>) RegisteredGlobalState.getState(_finishedPartitionsId);
    }

    private Map<Integer, Boolean> getDisabledStatuses() {
        return (Map<Integer, Boolean>) RegisteredGlobalState.getState(_disabledId);
    }
}
