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

import java.util.List;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.planner.ProcessorContext;
import org.apache.storm.trident.planner.TupleReceiver;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTuple.Factory;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.trident.tuple.TridentTupleView.OperationOutputFactory;


public class AppendCollector implements TridentCollector {
    OperationOutputFactory _factory;
    TridentContext _triContext;
    TridentTuple tuple;
    ProcessorContext context;
    
    public AppendCollector(TridentContext context) {
        _triContext = context;
        _factory = new OperationOutputFactory(context.getParentTupleFactories().get(0), context.getSelfOutputFields());
    }
                
    public void setContext(ProcessorContext pc, TridentTuple t) {
        this.context = pc;
        this.tuple = t;
    }

    @Override
    public void emit(List<Object> values) {
        TridentTuple toEmit = _factory.create((TridentTupleView) tuple, values);
        for(TupleReceiver r: _triContext.getReceivers()) {
            r.execute(context, _triContext.getOutStreamId(), toEmit);
        }
    }

    @Override
    public void reportError(Throwable t) {
        _triContext.getDelegateCollector().reportError(t);
    } 
    
    public Factory getOutputFactory() {
        return _factory;
    }
}
