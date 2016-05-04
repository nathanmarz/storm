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
package org.apache.storm.trident.operation.builtin;

import org.apache.storm.tuple.Values;
import java.util.ArrayList;
import java.util.List;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.state.snapshot.ReadOnlySnapshottable;
import org.apache.storm.trident.tuple.TridentTuple;

public class SnapshotGet extends BaseQueryFunction<ReadOnlySnapshottable, Object> {

    @Override
    public List<Object> batchRetrieve(ReadOnlySnapshottable state, List<TridentTuple> args) {
        List<Object> ret = new ArrayList<>(args.size());
        Object snapshot = state.get();
        for (TridentTuple arg : args) {
            ret.add(snapshot);
        }
        return ret;
    }

    @Override
    public void execute(TridentTuple tuple, Object result, TridentCollector collector) {
        collector.emit(new Values(result));
    }
}
