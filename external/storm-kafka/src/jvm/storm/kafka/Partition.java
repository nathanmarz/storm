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

import com.google.common.base.Objects;
import storm.trident.spout.ISpoutPartition;


public class Partition implements ISpoutPartition {

    public Broker host;
    public int partition;
    public String topic;

    //Flag to keep the Partition Path Id backward compatible with Old implementation of Partition.getId() == "partition_" + partition
    private Boolean bUseTopicNameForPartitionPathId;

    // for kryo compatibility
    private Partition() {
	
    }
    public Partition(Broker host, String topic, int partition) {
        this.topic = topic;
        this.host = host;
        this.partition = partition;
        this.bUseTopicNameForPartitionPathId = false;
    }
    
    public Partition(Broker host, String topic, int partition,Boolean bUseTopicNameForPartitionPathId) {
        this.topic = topic;
        this.host = host;
        this.partition = partition;
        this.bUseTopicNameForPartitionPathId = bUseTopicNameForPartitionPathId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(host, topic, partition);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Partition other = (Partition) obj;
        return Objects.equal(this.host, other.host) && Objects.equal(this.topic, other.topic) && Objects.equal(this.partition, other.partition);
    }

    @Override
    public String toString() {
        return "Partition{" +
                "host=" + host +
                ", topic=" + topic +
                ", partition=" + partition +
                '}';
    }

    @Override
    public String getId() {
        if (bUseTopicNameForPartitionPathId) {
            return  topic  + "/partition_" + partition;
        } else {
            //Keep the Partition Id backward compatible with Old implementation of Partition.getId() == "partition_" + partition
            return "partition_" + partition;
        }
    }

}
