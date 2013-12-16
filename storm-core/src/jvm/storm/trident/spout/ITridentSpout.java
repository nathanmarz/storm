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
import java.io.Serializable;
import java.util.Map;
import storm.trident.operation.TridentCollector;


public interface ITridentSpout<T> extends Serializable {
    public interface BatchCoordinator<X> {
        /**
         * Create metadata for this particular transaction id which has never
         * been emitted before. The metadata should contain whatever is necessary
         * to be able to replay the exact batch for the transaction at a later point.
         * 
         * The metadata is stored in Zookeeper.
         * 
         * Storm uses the Kryo serializations configured in the component configuration 
         * for this spout to serialize and deserialize the metadata.
         * 
         * @param txid The id of the transaction.
         * @param prevMetadata The metadata of the previous transaction
         * @param currMetadata The metadata for this transaction the last time it was initialized.
         *                     null if this is the first attempt
         * @return the metadata for this new transaction
         */
        X initializeTransaction(long txid, X prevMetadata, X currMetadata);
        
        void success(long txid);
        
        boolean isReady(long txid);
        
        /**
         * Release any resources from this coordinator.
         */
        void close();
    }
    
    public interface Emitter<X> {
        /**
         * Emit a batch for the specified transaction attempt and metadata for the transaction. The metadata
         * was created by the Coordinator in the initializeTranaction method. This method must always emit
         * the same batch of tuples across all tasks for the same transaction id.
         * 
         */
        void emitBatch(TransactionAttempt tx, X coordinatorMeta, TridentCollector collector);
        
        /**
         * This attempt committed successfully, so all state for this commit and before can be safely cleaned up.
         */
        void success(TransactionAttempt tx);
        
        /**
         * Release any resources held by this emitter.
         */
        void close();
    }
    
    /**
     * The coordinator for a TransactionalSpout runs in a single thread and indicates when batches
     * of tuples should be emitted and when transactions should commit. The Coordinator that you provide 
     * in a TransactionalSpout provides metadata for each transaction so that the transactions can be replayed.
     */
    BatchCoordinator<T> getCoordinator(String txStateId, Map conf, TopologyContext context);

    /**
     * The emitter for a TransactionalSpout runs as many tasks across the cluster. Emitters are responsible for
     * emitting batches of tuples for a transaction and must ensure that the same batch of tuples is always
     * emitted for the same transaction id.
     */    
    Emitter<T> getEmitter(String txStateId, Map conf, TopologyContext context); 
    
    Map getComponentConfiguration();
    Fields getOutputFields();
}
