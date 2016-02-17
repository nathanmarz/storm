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

import java.util.Comparator;

/**
 * This aggregator computes the maximum of aggregated tuples in a stream. It uses given {@code comparator} for comparing
 * two values in a stream.
 *
 */
public class MaxWithComparator<T> extends ComparisonAggregator<T> {
    private final Comparator<T> comparator;
    
    public MaxWithComparator(Comparator<T> comparator) {
        this(null, comparator);
    }

    public MaxWithComparator(String inputFieldName, Comparator<T> comparator) {
        super(inputFieldName);
        this.comparator = comparator;
    }

    @Override
    protected T compare(T value1, T value2) {
        return comparator.compare(value1, value2) > 0 ? value1 : value2;
    }

    @Override
    public String toString() {
        return "MaxWithComparator{" +
                "comparator=" + comparator +
                '}';
    }
}
