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
package storm.trident.operation.impl;

import java.io.Serializable;
import java.util.Map;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.operation.impl.SingleEmitAggregator.SingleEmitState;
import storm.trident.tuple.TridentTuple;


public class SingleEmitAggregator implements Aggregator<SingleEmitState> {
    public static interface BatchToPartition extends Serializable {
        int partitionIndex(Object batchId, int numPartitions);
    }
    
    static class SingleEmitState {
        boolean received = false;
        Object state;
        Object batchId;
        
        public SingleEmitState(Object batchId) {
            this.batchId = batchId;
        }
    }
    
    Aggregator _agg;
    BatchToPartition _batchToPartition;
    
    public SingleEmitAggregator(Aggregator agg, BatchToPartition batchToPartition) {
        _agg = agg;
        _batchToPartition = batchToPartition;
    }
    
    
    @Override
    public SingleEmitState init(Object batchId, TridentCollector collector) {
        return new SingleEmitState(batchId);
    }

    @Override
    public void aggregate(SingleEmitState val, TridentTuple tuple, TridentCollector collector) {
        if(!val.received) {
            val.state = _agg.init(val.batchId, collector);
            val.received = true;
        }
        _agg.aggregate(val.state, tuple, collector);
    }

    @Override
    public void complete(SingleEmitState val, TridentCollector collector) {
        if(!val.received) {
            if(this.myPartitionIndex == _batchToPartition.partitionIndex(val.batchId, this.totalPartitions)) {
                val.state = _agg.init(val.batchId, collector);
                _agg.complete(val.state, collector);
            }
        } else {
            _agg.complete(val.state, collector);
        }
    }

    int myPartitionIndex;
    int totalPartitions;
    
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        _agg.prepare(conf, context);
        this.myPartitionIndex = context.getPartitionIndex();
        this.totalPartitions = context.numPartitions();
    }

    @Override
    public void cleanup() {
        _agg.cleanup();
    }
    
    
}
