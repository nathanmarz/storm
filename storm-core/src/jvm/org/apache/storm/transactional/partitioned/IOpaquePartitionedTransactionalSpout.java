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
package org.apache.storm.transactional.partitioned;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IComponent;
import org.apache.storm.transactional.TransactionAttempt;
import java.util.Map;

/**
 * This defines a transactional spout which does *not* necessarily
 * replay the same batch every time it emits a batch for a transaction id.
 */
public interface IOpaquePartitionedTransactionalSpout<T> extends IComponent {
    public interface Coordinator {
        /**
         * Returns true if its ok to emit start a new transaction, false otherwise (will skip this transaction).
         * 
         * You should sleep here if you want a delay between asking for the next transaction (this will be called 
         * repeatedly in a loop).
         */
        boolean isReady();
        void close();
    }
    
    public interface Emitter<X> {
        /**
         * Emit a batch of tuples for a partition/transaction. 
         * 
         * Return the metadata describing this batch that will be used as lastPartitionMeta
         * for defining the parameters of the next batch.
         */
        X emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition, X lastPartitionMeta);
        int numPartitions();
        void close();
    }
    
    Emitter<T> getEmitter(Map conf, TopologyContext context);     
    Coordinator getCoordinator(Map conf, TopologyContext context);     
}
