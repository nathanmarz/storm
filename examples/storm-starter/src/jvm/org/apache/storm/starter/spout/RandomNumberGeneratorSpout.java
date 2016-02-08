/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.starter.spout;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This spout generates random whole numbers with given {@code maxNumber} value as maximum with the given {@code fields}.
 *
 */
public class RandomNumberGeneratorSpout implements IBatchSpout {
    private final Fields fields;
    private final int batchSize;
    private final int maxNumber;
    private final Map<Long, List<List<Object>>> batches = new HashMap<>();

    public RandomNumberGeneratorSpout(Fields fields, int batchSize, int maxNumber) {
        this.fields = fields;
        this.batchSize = batchSize;
        this.maxNumber = maxNumber;
    }

    @Override
    public void open(Map conf, TopologyContext context) {
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        List<List<Object>> values = null;
        if(batches.containsKey(batchId)) {
            values = batches.get(batchId);
        } else {
            values = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                List<Object> numbers = new ArrayList<>();
                for (int x=0; x<fields.size(); x++) {
                    numbers.add(ThreadLocalRandom.current().nextInt(0, maxNumber + 1));
                }
                values.add(numbers);
            }
            batches.put(batchId, values);
        }
        for (List<Object> value : values) {
            collector.emit(value);
        }
    }

    @Override
    public void ack(long batchId) {
        batches.remove(batchId);
    }

    @Override
    public void close() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    @Override
    public Fields getOutputFields() {
        return fields;
    }
}
