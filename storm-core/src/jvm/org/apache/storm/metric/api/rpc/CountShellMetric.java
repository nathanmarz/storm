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
package org.apache.storm.metric.api.rpc;

import org.apache.storm.metric.api.CountMetric;

public class CountShellMetric extends CountMetric implements IShellMetric {
    /***
     * @param value should be null or long
     *  if value is null, it will call incr()
     *  if value is long, it will call incrBy((long)params)
     * */
    public void updateMetricFromRPC(Object value) {
        if (value == null) {
            incr();
        } else if (value instanceof Long) {
            incrBy((Long)value);
        } else {
            throw new RuntimeException("CountShellMetric updateMetricFromRPC params should be null or Long");
        }
    }
}
