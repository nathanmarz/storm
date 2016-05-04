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
package org.apache.storm.metric.api;

class MeanReducerState {
    public int count = 0;
    public double sum = 0.0;
}

public class MeanReducer implements IReducer<MeanReducerState> {
    public MeanReducerState init() {
        return new MeanReducerState();
    }

    public MeanReducerState reduce(MeanReducerState acc, Object input) {
        acc.count++;
        if(input instanceof Double) {
            acc.sum += (Double)input;
        } else if(input instanceof Long) {
            acc.sum += ((Long)input).doubleValue();
        } else if(input instanceof Integer) {
            acc.sum += ((Integer)input).doubleValue();
        } else {
            throw new RuntimeException(
                "MeanReducer::reduce called with unsupported input type `" + input.getClass()
                + "`. Supported types are Double, Long, Integer.");
        }
        return acc;
    }

    public Object extractResult(MeanReducerState acc) {
        if(acc.count > 0) {
            return acc.sum / (double) acc.count;
        } else {
            return null;
        }
    }
}
