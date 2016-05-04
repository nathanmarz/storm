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
package org.apache.storm.trident.planner;

import org.apache.storm.task.TopologyContext;
import java.io.Serializable;
import java.util.Map;
import org.apache.storm.trident.planner.processor.TridentContext;
import org.apache.storm.trident.tuple.TridentTuple.Factory;

public interface TridentProcessor extends Serializable, TupleReceiver {
    
    // imperative that don't emit any tuples from here, since output factory cannot be gotten until
    // preparation is done, therefore, receivers won't be ready to receive tuples yet
    // can't emit tuples from here anyway, since it's not within a batch context (which is only
    // startBatch, execute, and finishBatch
    void prepare(Map conf, TopologyContext context, TridentContext tridentContext);
    void cleanup();
    
    void startBatch(ProcessorContext processorContext);
    
    void finishBatch(ProcessorContext processorContext);
    
    Factory getOutputFactory();
}
