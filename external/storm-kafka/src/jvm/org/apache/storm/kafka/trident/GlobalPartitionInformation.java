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

import com.google.common.base.Objects;
import org.apache.storm.kafka.Broker;
import org.apache.storm.kafka.Partition;

import java.io.Serializable;
import java.util.*;


public class GlobalPartitionInformation implements Iterable<Partition>, Serializable {

    private Map<Integer, Broker> partitionMap;
    public String topic;

    //Flag to keep the Partition Path Id backward compatible with Old implementation of Partition.getId() == "partition_" + partition
    private Boolean bUseTopicNameForPartitionPathId;

    public GlobalPartitionInformation(String topic, Boolean bUseTopicNameForPartitionPathId) {
        this.topic = topic;
        this.partitionMap = new TreeMap<Integer, Broker>();
        this.bUseTopicNameForPartitionPathId = bUseTopicNameForPartitionPathId;
    }

    public GlobalPartitionInformation(String topic) {
        this.topic = topic;
        this.partitionMap = new TreeMap<Integer, Broker>();
        this.bUseTopicNameForPartitionPathId = false;
    }

    public void addPartition(int partitionId, Broker broker) {
        partitionMap.put(partitionId, broker);
    }

    @Override
    public String toString() {
        return "GlobalPartitionInformation{" +
                "topic=" + topic +
                ", partitionMap=" + partitionMap +
                '}';
    }

    public Broker getBrokerFor(Integer partitionId) {
        return partitionMap.get(partitionId);
    }

    public List<Partition> getOrderedPartitions() {
        List<Partition> partitions = new LinkedList<Partition>();
        for (Map.Entry<Integer, Broker> partition : partitionMap.entrySet()) {
            partitions.add(new Partition(partition.getValue(), this.topic, partition.getKey(), this.bUseTopicNameForPartitionPathId));
        }
        return partitions;
    }

    @Override
    public Iterator<Partition> iterator() {
        final Iterator<Map.Entry<Integer, Broker>> iterator = partitionMap.entrySet().iterator();
        final String topic = this.topic;
        final Boolean bUseTopicNameForPartitionPathId = this.bUseTopicNameForPartitionPathId;
        return new Iterator<Partition>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Partition next() {
                Map.Entry<Integer, Broker> next = iterator.next();
                return new Partition(next.getValue(), topic , next.getKey(), bUseTopicNameForPartitionPathId);
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(partitionMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final GlobalPartitionInformation other = (GlobalPartitionInformation) obj;
        return Objects.equal(this.partitionMap, other.partitionMap);
    }
}
