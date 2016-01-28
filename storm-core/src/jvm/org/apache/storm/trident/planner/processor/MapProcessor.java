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

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.FlatMapFunction;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.planner.ProcessorContext;
import org.apache.storm.trident.planner.TridentProcessor;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;

import java.util.List;
import java.util.Map;

/**
 * Processor for executing {@link org.apache.storm.trident.Stream#map(MapFunction)} and
 * {@link org.apache.storm.trident.Stream#flatMap(FlatMapFunction)} functions.
 */
public class MapProcessor implements TridentProcessor {
    Function _function;
    TridentContext _context;
    FreshCollector _collector;
    Fields _inputFields;
    TridentTupleView.ProjectionFactory _projection;

    public MapProcessor(Fields inputFields, Function function) {
        _function = function;
        _inputFields = inputFields;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, TridentContext tridentContext) {
        List<TridentTuple.Factory> parents = tridentContext.getParentTupleFactories();
        if(parents.size()!=1) {
            throw new RuntimeException("Map operation can only have one parent");
        }
        _context = tridentContext;
        _collector = new FreshCollector(tridentContext);
        _projection = new TridentTupleView.ProjectionFactory(parents.get(0), _inputFields);
        _function.prepare(conf, new TridentOperationContext(context, _projection));
    }

    @Override
    public void cleanup() {
        _function.cleanup();
    }

    @Override
    public void execute(ProcessorContext processorContext, String streamId, TridentTuple tuple) {
        _collector.setContext(processorContext);
        _function.execute(_projection.create(tuple), _collector);
    }

    @Override
    public void startBatch(ProcessorContext processorContext) {
        // NOOP
    }

    @Override
    public void finishBatch(ProcessorContext processorContext) {
        // NOOP
    }

    @Override
    public TridentTuple.Factory getOutputFactory() {
        return _collector.getOutputFactory();
    }
}
