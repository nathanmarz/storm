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

public class ReducedMetric implements IMetric {
    private final IReducer _reducer;
    private Object _accumulator;

    public ReducedMetric(IReducer reducer) {
        _reducer = reducer;
        _accumulator = _reducer.init();
    }

    public void update(Object value) {
        _accumulator = _reducer.reduce(_accumulator, value);
    }

    public Object getValueAndReset() {
        Object ret = _reducer.extractResult(_accumulator);
        _accumulator = _reducer.init();
        return ret;
    }
}
