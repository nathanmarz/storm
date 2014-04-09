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

    public final Broker host;
    public final int partition;

    public Partition(Broker host, int partition) {
        this.host = host;
        this.partition = partition;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(host, partition);
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
        return Objects.equal(this.host, other.host) && Objects.equal(this.partition, other.partition);
    }

    @Override
    public String toString() {
        return "Partition{" +
                "host=" + host +
                ", partition=" + partition +
                '}';
    }

    @Override
    public String getId() {
        return "partition_" + partition;
    }

}
