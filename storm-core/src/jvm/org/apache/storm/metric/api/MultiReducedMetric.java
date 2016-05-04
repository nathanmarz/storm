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

import org.apache.storm.metric.api.IMetric;
import java.util.HashMap;
import java.util.Map;

public class MultiReducedMetric implements IMetric {
    Map<String, ReducedMetric> _value = new HashMap<>();
    IReducer _reducer;

    public MultiReducedMetric(IReducer reducer) {
        _reducer = reducer;
    }
    
    public ReducedMetric scope(String key) {
        ReducedMetric val = _value.get(key);
        if(val == null) {
            _value.put(key, val = new ReducedMetric(_reducer));
        }
        return val;
    }

    public Object getValueAndReset() {
        Map ret = new HashMap();
        for(Map.Entry<String, ReducedMetric> e : _value.entrySet()) {
            Object val = e.getValue().getValueAndReset();
            if(val != null) {
                ret.put(e.getKey(), val);
            }
        }
        return ret;
    }
}
