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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class StaticBrokerReader implements IBrokerReader {

    private Map<String,GlobalPartitionInformation> brokers = new TreeMap<String,GlobalPartitionInformation>();

    public StaticBrokerReader(String topic, GlobalPartitionInformation partitionInformation) {
        this.brokers.put(topic, partitionInformation);
    }

    @Override
    public GlobalPartitionInformation getBrokerForTopic(String topic) {
        if (brokers.containsKey(topic)) return brokers.get(topic);
        return null;
    }

    @Override
    public List<GlobalPartitionInformation> getAllBrokers () {
        List<GlobalPartitionInformation> list = new ArrayList<GlobalPartitionInformation>();
        list.addAll(brokers.values());
        return list;
    }

    @Override
    public void close() {
    }
}
