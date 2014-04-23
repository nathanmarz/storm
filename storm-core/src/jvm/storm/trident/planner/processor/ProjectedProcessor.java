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
package storm.trident.planner.processor;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.util.Map;
import storm.trident.planner.ProcessorContext;
import storm.trident.planner.TridentProcessor;
import storm.trident.planner.TupleReceiver;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTuple.Factory;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;


public class ProjectedProcessor implements TridentProcessor {
    Fields _projectFields;
    ProjectionFactory _factory;
    TridentContext _context;
    
    public ProjectedProcessor(Fields projectFields) {
        _projectFields = projectFields;
    }
    
    @Override
    public void prepare(Map conf, TopologyContext context, TridentContext tridentContext) {
        if(tridentContext.getParentTupleFactories().size()!=1) {
            throw new RuntimeException("Projection processor can only have one parent");
        }
        _context = tridentContext;
        _factory = new ProjectionFactory(tridentContext.getParentTupleFactories().get(0), _projectFields);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
    }

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        TridentTuple toEmit = _factory.create(tuple);
        for(TupleReceiver r: _context.getReceivers()) {
            r.execute(processorContext, _context.getOutStreamId(), toEmit);
        }
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {
    }

    @Override
    public Factory getOutputFactory() {
        return _factory;
    }
}
