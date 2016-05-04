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
package org.apache.storm.trident.planner.processor;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.tuple.Fields;
import java.util.List;
import org.apache.storm.trident.planner.TupleReceiver;
import org.apache.storm.trident.tuple.TridentTuple.Factory;


public class TridentContext {
    Fields selfFields;
    List<Factory> parentFactories;
    List<String> parentStreams;
    List<TupleReceiver> receivers;
    String outStreamId;
    int stateIndex;
    BatchOutputCollector collector;
    
    public TridentContext(Fields selfFields, List<Factory> parentFactories,
            List<String> parentStreams, List<TupleReceiver> receivers, 
            String outStreamId, int stateIndex, BatchOutputCollector collector) {
        this.selfFields = selfFields;
        this.parentFactories = parentFactories;
        this.parentStreams = parentStreams;
        this.receivers = receivers;
        this.outStreamId = outStreamId;
        this.stateIndex = stateIndex;
        this.collector = collector;        
    }
    
    public List<Factory> getParentTupleFactories() {
        return parentFactories;
    }
    
    public Fields getSelfOutputFields() {
        return selfFields;
    }
    
    public List<String> getParentStreams() {
        return parentStreams;
    }
    
    public List<TupleReceiver> getReceivers() {
        return receivers;
    }
    
    public String getOutStreamId() {
        return outStreamId;
    }
    
    public int getStateIndex() {
        return stateIndex;
    }
    
    //for reporting errors
    public BatchOutputCollector getDelegateCollector() {
        return collector;
    }
}
