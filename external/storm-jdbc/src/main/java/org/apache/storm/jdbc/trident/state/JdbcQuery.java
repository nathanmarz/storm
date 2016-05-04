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
package org.apache.storm.jdbc.trident.state;

import org.apache.storm.tuple.Values;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;

public class JdbcQuery extends BaseQueryFunction<JdbcState, List<Values>> {

    @Override
    public List<List<Values>> batchRetrieve(JdbcState jdbcState, List<TridentTuple> tridentTuples) {
        return jdbcState.batchRetrieve(tridentTuples);
    }

    @Override
    public void execute(TridentTuple tuples, List<Values> values, TridentCollector tridentCollector) {
        for (Values value : values) {
            tridentCollector.emit(value);
        }
    }
}
