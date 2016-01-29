/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.trident.operation.builtin;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * Abstract {@code Aggregator} for comparing two values in a stream.
 *
 */
public abstract class ComparisonAggregator<T> extends BaseAggregator<ComparisonAggregator.State> {

    public static class State {
        TridentTuple previousTuple;
    }

    private final String inputFieldName;

    public ComparisonAggregator(String inputFieldName) {
        this.inputFieldName = inputFieldName;
    }

    protected abstract T compare(T value1, T value2);

    @Override
    public State init(Object batchId, TridentCollector collector) {
        return new State();
    }

    @Override
    public void aggregate(State state, TridentTuple tuple, TridentCollector collector) {
        T value1 = valueFromTuple(state.previousTuple);
        T value2 = valueFromTuple(tuple);

        if(value2 == null) {
            return;
        }

        if(value1 == null || compare(value1, value2) == value2) {
            state.previousTuple = tuple;
        }

    }

    protected T valueFromTuple(TridentTuple tuple) {
        // when there is no input field then the whole tuple is considered for comparison.
        return (T) (inputFieldName != null && tuple != null ? tuple.getValueByField(inputFieldName) : tuple);
    }

    @Override
    public void complete(State state, TridentCollector collector) {
        collector.emit(state.previousTuple.getValues());
    }
}
