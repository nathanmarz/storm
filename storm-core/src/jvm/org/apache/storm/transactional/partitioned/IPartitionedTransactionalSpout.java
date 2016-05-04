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

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IComponent;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.coordination.BatchOutputCollector;
import java.util.Map;

/**
 * This interface defines a transactional spout that reads its tuples from a partitioned set of 
 * brokers. It automates the storing of metadata for each partition to ensure that the same batch
 * is always emitted for the same transaction id. The partition metadata is stored in Zookeeper.
 */
public interface IPartitionedTransactionalSpout<T> extends IComponent {
    public interface Coordinator {
        /**
         * Return the number of partitions currently in the source of data. The idea is
         * is that if a new partition is added and a prior transaction is replayed, it doesn't
         * emit tuples for the new partition because it knows how many partitions were in 
         * that transaction.
         */
        int numPartitions();
        
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
         * Emit a batch of tuples for a partition/transaction that's never been emitted before.
         * Return the metadata that can be used to reconstruct this partition/batch in the future.
         */
        X emitPartitionBatchNew(TransactionAttempt tx, BatchOutputCollector collector, int partition, X lastPartitionMeta);

        /**
         * Emit a batch of tuples for a partition/transaction that has been emitted before, using
         * the metadata created when it was first emitted.
         */
        void emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition, X partitionMeta);
        void close();
    }
    
    Coordinator getCoordinator(Map conf, TopologyContext context);
    Emitter<T> getEmitter(Map conf, TopologyContext context);      
}
