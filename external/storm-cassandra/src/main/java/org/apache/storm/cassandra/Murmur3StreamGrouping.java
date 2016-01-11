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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.cassandra;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.tuple.Fields;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * Simple {@link org.apache.storm.grouping.CustomStreamGrouping} that uses Murmur3 algorithm to choose the target task of a tuple.
 *
 * This stream grouping may be used to optimise writes to Apache Cassandra.
 */
public class Murmur3StreamGrouping implements CustomStreamGrouping {

    private List<Integer> targetTasks;

    private List<Integer> partitionKeyIndexes;

    /**
     * A list of partition key. The order of specified keys will be used to generate the partition key hash.
     * It should respect the column order defined into the targeted CQL table.
     */
    private List<String> partitionKeyNames;

    /**
     * Creates a new {@link Murmur3StreamGrouping} instance.
     * @param partitionKeyNames {@link org.apache.storm.cassandra.Murmur3StreamGrouping#partitionKeyNames}.
     */
    public Murmur3StreamGrouping(String...partitionKeyNames) {
        this( Arrays.asList(partitionKeyNames));
    }

    /**
     * Creates a new {@link Murmur3StreamGrouping} instance.
     * @param partitionKeyNames {@link org.apache.storm.cassandra.Murmur3StreamGrouping#partitionKeyNames}.
     */
    public Murmur3StreamGrouping(List<String> partitionKeyNames) {
        this.partitionKeyNames = partitionKeyNames;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;

        this.partitionKeyIndexes = new ArrayList<>();
        Fields componentOutputFields = context.getComponentOutputFields(stream);
        for (String partitionKeyName : partitionKeyNames) {
            partitionKeyIndexes.add(componentOutputFields.fieldIndex(partitionKeyName));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        try {
            int n = Math.abs( (int) hashes(getKeyValues(values)) % targetTasks.size() );
            return Lists.newArrayList(targetTasks.get(n));
        } catch (IOException e) {
            throw new FailedException(e);
        }
    }

    private List<Object> getKeyValues(List<Object> values) {
        List<Object> keys = new ArrayList<>();
        for(Integer idx : partitionKeyIndexes) {
            keys.add(values.get(idx));
        }
        return keys;
    }

    /**
     * Computes the murmur3 hash for the specified values.
     * http://stackoverflow.com/questions/27212797/cassandra-hashing-algorithm-with-composite-keys
     * https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/marshal/CompositeType.java
     *
     * @param values the fields which are part of the (compose) partition key.
     * @return the computed hash for input values.
     * @throws java.io.IOException
     */
    @VisibleForTesting
    public static long hashes(List<Object> values) throws IOException {
        byte[] keyBytes;
        try(ByteArrayOutputStream bos = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(bos)) {
            for(Object key : values) {
                byte[] arr = ((String)key).getBytes("UTF-8");
                out.writeShort(arr.length);
                out.write(arr, 0, arr.length);
                out.writeByte(0);
            }
            out.flush();
            keyBytes = bos.toByteArray();
        }
        return Hashing.murmur3_128().hashBytes(keyBytes).asLong();
    }
}
