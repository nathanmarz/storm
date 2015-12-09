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
package storm.kafka.trident;

import backtype.storm.task.IMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.trident.mapper.TridentTupleToKafkaMapper;
import storm.kafka.trident.selector.KafkaTopicSelector;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;
import java.util.Properties;

public class TridentKafkaStateFactory implements StateFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TridentKafkaStateFactory.class);

    private TridentTupleToKafkaMapper mapper;
    private KafkaTopicSelector topicSelector;
    private Properties producerProperties = new Properties();

    public TridentKafkaStateFactory withTridentTupleToKafkaMapper(TridentTupleToKafkaMapper mapper) {
        this.mapper = mapper;
        return this;
    }

    public TridentKafkaStateFactory withKafkaTopicSelector(KafkaTopicSelector selector) {
        this.topicSelector = selector;
        return this;
    }

    public TridentKafkaStateFactory withProducerProperties(Properties props) {
        this.producerProperties = props;
        return this;
    }

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        LOG.info("makeState(partitonIndex={}, numpartitions={}", partitionIndex, numPartitions);
        TridentKafkaState state = new TridentKafkaState()
                .withKafkaTopicSelector(this.topicSelector)
                .withTridentTupleToKafkaMapper(this.mapper);
        state.prepare(producerProperties);
        return state;
    }
}
