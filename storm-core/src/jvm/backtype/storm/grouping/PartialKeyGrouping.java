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
package backtype.storm.grouping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class PartialKeyGrouping implements CustomStreamGrouping, Serializable {
    private static final long serialVersionUID = -447379837314000353L;
    private List<Integer> targetTasks;
    private long[] targetTaskStats;
    private HashFunction h1 = Hashing.murmur3_128(13);
    private HashFunction h2 = Hashing.murmur3_128(17);

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
        targetTaskStats = new long[this.targetTasks.size()];
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<Integer>(1);
        if (values.size() > 0) {
            String str = values.get(0).toString(); // assume key is the first field
            int firstChoice = (int) Math.abs(h1.hashBytes(str.getBytes()).asLong()) % this.targetTasks.size();
            int secondChoice = (int) Math.abs(h2.hashBytes(str.getBytes()).asLong()) % this.targetTasks.size();
            int selected = targetTaskStats[firstChoice] > targetTaskStats[secondChoice] ? secondChoice : firstChoice;
            boltIds.add(targetTasks.get(selected));
            targetTaskStats[selected]++;
        }
        return boltIds;
    }
}
