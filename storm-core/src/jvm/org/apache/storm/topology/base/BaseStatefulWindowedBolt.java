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
package org.apache.storm.topology.base;

import org.apache.storm.Config;
import org.apache.storm.state.State;
import org.apache.storm.topology.IStatefulWindowedBolt;

public abstract class BaseStatefulWindowedBolt<T extends State> extends BaseWindowedBolt implements IStatefulWindowedBolt<T> {
    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withWindow(Count windowLength, Count slidingInterval) {
        super.withWindow(windowLength, slidingInterval);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withWindow(Count windowLength, Duration slidingInterval) {
        super.withWindow(windowLength, slidingInterval);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withWindow(Duration windowLength, Count slidingInterval) {
        super.withWindow(windowLength, slidingInterval);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withWindow(Duration windowLength, Duration slidingInterval) {
        super.withWindow(windowLength, slidingInterval);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withWindow(Count windowLength) {
        super.withWindow(windowLength);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withWindow(Duration windowLength) {
        super.withWindow(windowLength);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withTumblingWindow(Count count) {
        super.withTumblingWindow(count);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withTumblingWindow(Duration duration) {
        super.withTumblingWindow(duration);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withTimestampField(String fieldName) {
        super.withTimestampField(fieldName);
        return this;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withLag(Duration duration) {
        super.withLag(duration);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseStatefulWindowedBolt<T> withWatermarkInterval(Duration interval) {
        super.withWatermarkInterval(interval);
        return this;
    }

    /**
     * Specify the name of the field in the tuple that holds the message id. This is used to track
     * the windowing boundaries and re-evaluating the windowing operation during recovery of IStatefulWindowedBolt
     *
     * @param fieldName the name of the field that contains the message id
     */
    public BaseStatefulWindowedBolt<T> withMessageIdField(String fieldName) {
        windowConfiguration.put(Config.TOPOLOGY_BOLTS_MESSAGE_ID_FIELD_NAME, fieldName);
        return this;
    }

    @Override
    public void preCommit(long txid) {
        // NOOP
    }

    @Override
    public void prePrepare(long txid) {
        // NOOP
    }

    @Override
    public void preRollback() {
        // NOOP
    }
}
