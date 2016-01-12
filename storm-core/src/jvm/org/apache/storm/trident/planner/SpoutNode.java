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
package org.apache.storm.trident.planner;

import org.apache.storm.tuple.Fields;


public class SpoutNode extends Node {
    public static enum SpoutType {
        DRPC,
        BATCH
    }
    
    public Object spout;
    public String txId; //where state is stored in zookeeper (only for batch spout types)
    public SpoutType type;
    
    public SpoutNode(String streamId, Fields allOutputFields, String txid, Object spout, SpoutType type) {
        super(streamId, null, allOutputFields);
        this.txId = txid;
        this.spout = spout;
        this.type = type;
    }
}
