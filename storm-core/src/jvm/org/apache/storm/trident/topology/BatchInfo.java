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
package org.apache.storm.trident.topology;

import org.apache.storm.trident.spout.IBatchID;


public class BatchInfo {
    public IBatchID batchId;
    public Object state;
    public String batchGroup;
    
    public BatchInfo(String batchGroup, IBatchID batchId, Object state) {
        this.batchGroup = batchGroup;
        this.batchId = batchId;
        this.state = state;
    }
}
