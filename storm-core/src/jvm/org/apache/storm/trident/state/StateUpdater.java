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
package org.apache.storm.trident.state;

import java.util.List;
import org.apache.storm.trident.operation.Operation;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;


public interface StateUpdater<S extends State> extends Operation {
    // maybe it needs a start phase (where it can do a retrieval, an update phase, and then a finish phase...?
    // shouldn't really be a one-at-a-time interface, since we have all the tuples already?
    // TOOD: used for the new values stream
    // the list is needed to be able to get reduceragg and combineragg persistentaggregate
    // for grouped streams working efficiently
    void updateState(S state, List<TridentTuple> tuples, TridentCollector collector);
}
