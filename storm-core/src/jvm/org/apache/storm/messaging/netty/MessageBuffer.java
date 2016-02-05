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
package org.apache.storm.messaging.netty;

import org.apache.storm.messaging.TaskMessage;

/**
 * Encapsulates the state used for batching up messages.
 */
public class MessageBuffer {
    private final int mesageBatchSize;
    private MessageBatch currentBatch;

    public MessageBuffer(int mesageBatchSize){
        this.mesageBatchSize = mesageBatchSize;
        this.currentBatch = new MessageBatch(mesageBatchSize);
    }

    public synchronized MessageBatch add(TaskMessage msg){
        currentBatch.add(msg);
        if(currentBatch.isFull()){
            MessageBatch ret = currentBatch;
            currentBatch = new MessageBatch(mesageBatchSize);
            return ret;
        } else {
            return null;
        }
    }

    public synchronized boolean isEmpty() {
        return currentBatch.isEmpty();
    }

    public synchronized MessageBatch drain() {
        if(!currentBatch.isEmpty()) {
            MessageBatch ret = currentBatch;
            currentBatch = new MessageBatch(mesageBatchSize);
            return ret;
        } else {
            return null;
        }
    }
}
