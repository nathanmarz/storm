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

import storm.trident.state.ITupleCollection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

public class TupleCollectionGet extends BaseQueryFunction<State, Iterator<List<Object>>> {

    @Override
    public List<Iterator<List<Object>>> batchRetrieve(State state, List<TridentTuple> args) {
        List<Iterator<List<Object>>> ret = new ArrayList(args.size());
        for(int i=0; i<args.size(); i++) {
            ret.add(((ITupleCollection)state).getTuples());
        }
        return ret;
    }

    @Override
    public void execute(TridentTuple tuple, Iterator<List<Object>> tuplesIterator, TridentCollector collector) {
        while(tuplesIterator.hasNext()) {
            collector.emit(tuplesIterator.next());
        }
    }
}
