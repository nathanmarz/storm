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
package org.apache.storm.messaging;

import org.apache.storm.grouping.Load;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public interface IConnection {
    /**
     * Register a callback to be notified when data is ready to be processed.
     * @param cb the callback to process the messages.
     */
    public void registerRecv(IConnectionCallback cb);

    /**
     * Send load metrics to all downstream connections.
     * @param taskToLoad a map from the task id to the load for that task.
     */
    public void sendLoadMetrics(Map<Integer, Double> taskToLoad);
    
    /**
     * send a message with taskId and payload
     * @param taskId task ID
     * @param payload
     */
    public void send(int taskId,  byte[] payload);
    
    /**
     * send batch messages
     * @param msgs
     */

    public void send(Iterator<TaskMessage> msgs);
    
    /**
     * Get the current load for the given tasks
     * @param tasks the tasks to look for.
     * @return a Load for each of the tasks it knows about.
     */
    public Map<Integer, Load> getLoad(Collection<Integer> tasks);

    /**
     * close this connection
     */
    public void close();
}
