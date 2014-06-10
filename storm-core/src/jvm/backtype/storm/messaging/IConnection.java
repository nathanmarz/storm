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
package backtype.storm.messaging;

import java.util.Iterator;

public interface IConnection {   
    
    /**
     * receive a batch message iterator (consists taskId and payload)
     * @param flags 0: block, 1: non-block
     * @return
     */
    public Iterator<TaskMessage> recv(int flags, int clientId);
    
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
     * close this connection
     */
    public void close();
}
