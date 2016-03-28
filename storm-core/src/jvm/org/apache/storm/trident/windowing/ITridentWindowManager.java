/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.trident.windowing;

import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Queue;

/**
 * Window manager to handle trident tuple events.
 */
public interface ITridentWindowManager {

    /**
     * This is invoked from {@code org.apache.storm.trident.planner.TridentProcessor}'s  prepare method. So any
     * initialization tasks can be done before the topology starts accepting tuples. For ex:
     * initialize window manager with any earlier stored tuples/triggers and start WindowManager
     */
    public void prepare();

    /**
     * This is invoked when from {@code org.apache.storm.trident.planner.TridentProcessor}'s  cleanup method. So, any
     * cleanup operations like clearing cache or close store connection etc can be done.
     */
    public void shutdown();

    /**
     * Add received batch of tuples to cache/store and add them to {@code WindowManager}
     *
     * @param batchId
     * @param tuples
     */
    public void addTuplesBatch(Object batchId, List<TridentTuple> tuples);

    /**
     * Returns pending triggers to be emitted.
     *
     * @return
     */
    public Queue<StoreBasedTridentWindowManager.TriggerResult> getPendingTriggers();

}
