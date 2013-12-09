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
package storm.trident.operation.builtin;

import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.List;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.snapshot.ReadOnlySnapshottable;
import storm.trident.tuple.TridentTuple;

public class SnapshotGet extends BaseQueryFunction<ReadOnlySnapshottable, Object> {

    @Override
    public List<Object> batchRetrieve(ReadOnlySnapshottable state, List<TridentTuple> args) {
        List<Object> ret = new ArrayList<Object>(args.size());
        Object snapshot = state.get();
        for(int i=0; i<args.size(); i++) {
            ret.add(snapshot);
        }
        return ret;
    }

    @Override
    public void execute(TridentTuple tuple, Object result, TridentCollector collector) {
        collector.emit(new Values(result));
    }
}
