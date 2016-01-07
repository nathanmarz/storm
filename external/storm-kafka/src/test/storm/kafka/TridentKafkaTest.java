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
package storm.kafka;

import backtype.storm.tuple.Fields;
import kafka.javaapi.consumer.SimpleConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import storm.kafka.trident.TridentKafkaState;
import storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.trident.mapper.TridentTupleToKafkaMapper;
import storm.kafka.trident.selector.DefaultTopicSelector;
import storm.kafka.trident.selector.KafkaTopicSelector;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;

import java.util.ArrayList;
import java.util.List;

public class TridentKafkaTest {
    private KafkaTestBroker broker;
    private TridentKafkaState state;
    private SimpleConsumer simpleConsumer;

    @Before
    public void setup() {
        broker = new KafkaTestBroker();
        simpleConsumer = TestUtils.getKafkaConsumer(broker);
        TridentTupleToKafkaMapper mapper = new FieldNameBasedTupleToKafkaMapper("key", "message");
        KafkaTopicSelector topicSelector = new DefaultTopicSelector(TestUtils.TOPIC);
        state = new TridentKafkaState()
                .withKafkaTopicSelector(topicSelector)
                .withTridentTupleToKafkaMapper(mapper);
        state.prepare(TestUtils.getProducerProperties(broker.getBrokerConnectionString()));
    }

    @Test
    public void testKeyValue() {
        String keyString = "key-123";
        String valString = "message-123";
        int batchSize = 10;

        List<TridentTuple> tridentTuples = generateTupleBatch(keyString, valString, batchSize);

        state.updateState(tridentTuples, null);

        for(int i = 0 ; i < batchSize ; i++) {
            TestUtils.verifyMessage(keyString, valString, broker, simpleConsumer);
        }
    }

    private List<TridentTuple> generateTupleBatch(String key, String message, int batchsize) {
        List<TridentTuple> batch = new ArrayList<>();
        for(int i =0 ; i < batchsize; i++) {
            batch.add(TridentTupleView.createFreshTuple(new Fields("key", "message"), key, message));
        }
        return batch;
    }

    @After
    public void shutdown() {
        simpleConsumer.close();
        broker.shutdown();
    }
}
