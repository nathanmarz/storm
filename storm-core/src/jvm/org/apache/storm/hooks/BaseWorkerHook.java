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
package org.apache.storm.hooks;

import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.Map;

/**
 * A BaseWorkerHook is a noop implementation of IWorkerHook. You
 * may extends this class and implement any and/or all methods you
 * need for your workers.
 */
public class BaseWorkerHook implements IWorkerHook, Serializable {
    private static final long serialVersionUID = 2589466485198339529L;

    /**
     * This method is called when a worker is started
     *
     * @param stormConf The Storm configuration for this worker
     * @param context This object can be used to get information about this worker's place within the topology
     */
    @Override
    public void start(Map stormConf, WorkerTopologyContext context) {
        // NOOP
    }

    /**
     * This method is called right before a worker shuts down
     */
    @Override
    public void shutdown() {
        // NOOP
    }
}
