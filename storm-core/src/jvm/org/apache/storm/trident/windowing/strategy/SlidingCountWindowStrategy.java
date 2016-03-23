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
package org.apache.storm.trident.windowing.strategy;

import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.windowing.CountEvictionPolicy;
import org.apache.storm.windowing.CountTriggerPolicy;
import org.apache.storm.windowing.EvictionPolicy;
import org.apache.storm.windowing.TriggerHandler;
import org.apache.storm.windowing.TriggerPolicy;

/**
 * This class represents sliding window strategy based on the sliding window count and sliding interval count from the
 * given {@code slidingCountWindow} configuration.
 */
public class SlidingCountWindowStrategy<T> extends BaseWindowStrategy<T> {

    public SlidingCountWindowStrategy(WindowConfig slidingCountWindow) {
        super(slidingCountWindow);
    }

    /**
     * Returns a {@code TriggerPolicy} which triggers for every count of given sliding window.
     *
     * @param triggerHandler
     * @param evictionPolicy
     * @return
     */
    @Override
    public TriggerPolicy<T> getTriggerPolicy(TriggerHandler triggerHandler, EvictionPolicy<T> evictionPolicy) {
        return new CountTriggerPolicy<>(windowConfig.getSlidingLength(), triggerHandler, evictionPolicy);
    }

    /**
     * Returns an {@code EvictionPolicy} instance which evicts elements after a count of given window length.
     *
     * @return
     */
    @Override
    public EvictionPolicy<T> getEvictionPolicy() {
        return new CountEvictionPolicy<>(windowConfig.getWindowLength());
    }
}
