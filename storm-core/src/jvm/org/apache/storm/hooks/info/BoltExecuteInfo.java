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

public class BoltExecuteInfo {
    public Tuple tuple;
    public int executingTaskId;
    public Long executeLatencyMs; // null if it wasn't sampled
    
    public BoltExecuteInfo(Tuple tuple, int executingTaskId, Long executeLatencyMs) {
        this.tuple = tuple;
        this.executingTaskId = executingTaskId;
        this.executeLatencyMs = executeLatencyMs;
    }

    public void applyOn(TopologyContext topologyContext) {
        for (ITaskHook hook : topologyContext.getHooks()) {
            hook.boltExecute(this);
        }
    }
}
