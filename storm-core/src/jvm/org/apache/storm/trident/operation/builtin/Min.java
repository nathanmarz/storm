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

/**
 * This aggregator computes the minimum of aggregated tuples in a stream. It assumes that the tuple has one value and
 * it is an instance of {@code Comparable}.
 *
 */
public class Min extends ComparisonAggregator<Comparable<Object>> {

    public Min(String inputFieldName) {
        super(inputFieldName);
    }

    @Override
    protected Comparable<Object> compare(Comparable<Object> value1, Comparable<Object> value2) {
        return value1.compareTo(value2) < 0 ? value1 : value2;
    }
}
