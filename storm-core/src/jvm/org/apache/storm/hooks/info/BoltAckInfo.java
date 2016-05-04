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
package org.apache.storm.hooks.info;

import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

public class BoltAckInfo {
    public Tuple tuple;
    public int ackingTaskId;
    public Long processLatencyMs; // null if it wasn't sampled
    
    public BoltAckInfo(Tuple tuple, int ackingTaskId, Long processLatencyMs) {
        this.tuple = tuple;
        this.ackingTaskId = ackingTaskId;
        this.processLatencyMs = processLatencyMs;
    }

    public void applyOn(TopologyContext topologyContext) {
        for (ITaskHook hook : topologyContext.getHooks()) {
            hook.boltAck(this);
        }
    }
}
