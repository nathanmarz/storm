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
package org.apache.storm.kafka.trident;

import org.apache.storm.kafka.KafkaUtils;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.apache.storm.trident.spout.IPartitionedTridentSpout;

import java.util.List;
import java.util.Map;

class Coordinator implements IPartitionedTridentSpout.Coordinator<List<GlobalPartitionInformation>>, IOpaquePartitionedTridentSpout.Coordinator<List<GlobalPartitionInformation>> {

    private IBrokerReader reader;
    private TridentKafkaConfig config;

    public Coordinator(Map conf, TridentKafkaConfig tridentKafkaConfig) {
        config = tridentKafkaConfig;
        reader = KafkaUtils.makeBrokerReader(conf, config);
    }

    @Override
    public void close() {
        config.coordinator.close();
    }

    @Override
    public boolean isReady(long txid) {
        return config.coordinator.isReady(txid);
    }

    @Override
    public List<GlobalPartitionInformation> getPartitionsForBatch() {
        return reader.getAllBrokers();
    }
}
