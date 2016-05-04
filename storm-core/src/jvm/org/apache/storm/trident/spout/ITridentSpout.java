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
package org.apache.storm.trident.spout;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import java.io.Serializable;
import java.util.Map;
import org.apache.storm.trident.operation.TridentCollector;


public interface ITridentSpout<T> extends ITridentDataSource {
    interface BatchCoordinator<X> {
        /**
         * Create metadata for this particular transaction id which has never
         * been emitted before. The metadata should contain whatever is necessary
         * to be able to replay the exact batch for the transaction at a later point.
         * 
         * The metadata is stored in Zookeeper.
         * 
         * Storm uses JSON encoding to store the metadata.  Only simple types
         * such as numbers, booleans, strings, lists, and maps should be used.
         * 
         * @param txid The id of the transaction.
         * @param prevMetadata The metadata of the previous transaction
         * @param currMetadata The metadata for this transaction the last time it was initialized.
         *                     null if this is the first attempt
         * @return the metadata for this new transaction
         */
        X initializeTransaction(long txid, X prevMetadata, X currMetadata);

        /**
         * This attempt committed successfully, so all state for this commit and before can be safely cleaned up.
         *
         * @param txid transaction id that completed
         */
        void success(long txid);

        /**
         * hint to Storm if the spout is ready for the transaction id
         *
         * @param txid the id of the transaction
         * @return true, if the spout is ready for the given transaction id
         */
        boolean isReady(long txid);
        
        /**
         * Release any resources from this coordinator.
         */
        void close();
    }
    
    interface Emitter<X> {
        /**
         * Emit a batch for the specified transaction attempt and metadata for the transaction. The metadata
         * was created by the Coordinator in the initializeTransaction method. This method must always emit
         * the same batch of tuples across all tasks for the same transaction id.
         * @param tx transaction id
         * @param coordinatorMeta metadata from the coordinator defining this transaction
         * @param collector output tuple collector
         */
        void emitBatch(TransactionAttempt tx, X coordinatorMeta, TridentCollector collector);
        
        /**
         * This attempt committed successfully, so all state for this commit and before can be safely cleaned up.
         * @param tx attempt object containing transaction id and attempt number
         */
        void success(TransactionAttempt tx);
        
        /**
         * Release any resources held by this emitter.
         */
        void close();
    }
    
    /**
     * The coordinator for a TransactionalSpout runs in a single thread and indicates when batches of tuples
     * should be emitted. The Coordinator that you provide in a TransactionalSpout provides metadata for each
     * transaction so that the transactions can be replayed in case of failure.
     *
     * Two instances are requested, one on the master batch coordinator where isReady() is called, and an instance
     * in the coordinator bolt which is used for all other operations. The two instances do not necessarily share a
     * worker JVM.
     *
     * @param txStateId stream id
     * @param conf Storm config map
     * @param context topology context
     * @return spout coordinator instance
     */
    BatchCoordinator<T> getCoordinator(String txStateId, Map conf, TopologyContext context);

    /**
     * The emitter for a TransactionalSpout runs as many tasks across the cluster. Emitters are responsible for
     * emitting batches of tuples for a transaction and must ensure that the same batch of tuples is always
     * emitted for the same transaction id.
     *
     * All emitter tasks get the same transaction metadata. The topology context parameter contains the instance
     * task id that can be used to distribute the work across the tasks.
     *
     * @param txStateId stream id
     * @param conf Storm config map
     * @param context topology context
     * @return spout emitter
     */
    Emitter<T> getEmitter(String txStateId, Map conf, TopologyContext context); 
    
    Map<String, Object> getComponentConfiguration();
    Fields getOutputFields();
}
