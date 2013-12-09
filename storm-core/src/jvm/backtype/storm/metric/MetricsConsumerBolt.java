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
package backtype.storm.metric;

import backtype.storm.Config;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IBolt;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import java.util.Collection;
import java.util.Map;

public class MetricsConsumerBolt implements IBolt {
    IMetricsConsumer _metricsConsumer;
    String _consumerClassName;
    OutputCollector _collector;
    Object _registrationArgument;

    public MetricsConsumerBolt(String consumerClassName, Object registrationArgument) {
        _consumerClassName = consumerClassName;
        _registrationArgument = registrationArgument;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            _metricsConsumer = (IMetricsConsumer)Class.forName(_consumerClassName).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate a class listed in config under section " +
                Config.TOPOLOGY_METRICS_CONSUMER_REGISTER + " with fully qualified name " + _consumerClassName, e);
        }
        _metricsConsumer.prepare(stormConf, _registrationArgument, context, (IErrorReporter)collector);
        _collector = collector;
    }
    
    @Override
    public void execute(Tuple input) {
        _metricsConsumer.handleDataPoints((IMetricsConsumer.TaskInfo)input.getValue(0), (Collection)input.getValue(1));
        _collector.ack(input);
    }

    @Override
    public void cleanup() {
        _metricsConsumer.cleanup();
    }
    
}
