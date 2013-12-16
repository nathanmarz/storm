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
package storm.trident.operation;

import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.ICombiner;
import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.IReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.IMetricsContext;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;

public class TridentOperationContext implements IMetricsContext{
    TridentTuple.Factory _factory;
    TopologyContext _topoContext;
    
    public TridentOperationContext(TopologyContext topoContext, TridentTuple.Factory factory) {
        _factory = factory;
        _topoContext = topoContext;
    }
    
    public TridentOperationContext(TridentOperationContext parent, TridentTuple.Factory factory) {
        this(parent._topoContext, factory);
    }    
    
    public ProjectionFactory makeProjectionFactory(Fields fields) {
        return new ProjectionFactory(_factory, fields);
    }
    
    public int numPartitions() {
        return _topoContext.getComponentTasks(_topoContext.getThisComponentId()).size();
    }
    
    public int getPartitionIndex() {
        return _topoContext.getThisTaskIndex();
    }

    public <T extends IMetric> T registerMetric(String name, T metric, int timeBucketSizeInSecs) {
        return _topoContext.registerMetric(name, metric, timeBucketSizeInSecs);
    }
    public ReducedMetric registerMetric(String name, IReducer reducer, int timeBucketSizeInSecs) {
        return _topoContext.registerMetric(name, new ReducedMetric(reducer), timeBucketSizeInSecs);
    }
    public CombinedMetric registerMetric(String name, ICombiner combiner, int timeBucketSizeInSecs) {
        return _topoContext.registerMetric(name, new CombinedMetric(combiner), timeBucketSizeInSecs);
    }
}
