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
package backtype.storm.windowing;

import backtype.storm.tuple.Tuple;

import java.util.List;

/**
 * Holds the expired, new and current tuples in a window.
 */
public class TupleWindowImpl implements TupleWindow {
    private final List<Tuple> tuples;
    private final List<Tuple> newTuples;
    private final List<Tuple> expiredTuples;

    public TupleWindowImpl(List<Tuple> tuples, List<Tuple> newTuples, List<Tuple> expiredTuples) {
        this.tuples = tuples;
        this.newTuples = newTuples;
        this.expiredTuples = expiredTuples;
    }

    @Override
    public List<Tuple> get() {
        return tuples;
    }

    @Override
    public List<Tuple> getNew() {
        return newTuples;
    }

    @Override
    public List<Tuple> getExpired() {
        return expiredTuples;
    }

    @Override
    public String toString() {
        return "TupleWindowImpl{" +
                "tuples=" + tuples +
                ", newTuples=" + newTuples +
                ", expiredTuples=" + expiredTuples +
                '}';
    }
}
