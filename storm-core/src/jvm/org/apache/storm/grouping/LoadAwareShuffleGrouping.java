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
package org.apache.storm.grouping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.WorkerTopologyContext;

public class LoadAwareShuffleGrouping implements LoadAwareCustomStreamGrouping, Serializable {
    private Random random;
    private List<Integer>[] rets;
    private int[] targets;
    private int[] loads;
    private int total;
    private long lastUpdate = 0;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        random = new Random();
        rets = (List<Integer>[])new List<?>[targetTasks.size()];
        targets = new int[targetTasks.size()];
        for (int i = 0; i < targets.length; i++) {
            rets[i] = Arrays.asList(targetTasks.get(i));
            targets[i] = targetTasks.get(i);
        }
        loads = new int[targets.length];
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        throw new RuntimeException("NOT IMPLEMENTED");
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values, LoadMapping load) {
        if ((lastUpdate + 1000) < System.currentTimeMillis()) {
            int local_total = 0;
            for (int i = 0; i < targets.length; i++) {
                int val = (int)(101 - (load.get(targets[i]) * 100));
                loads[i] = val;
                local_total += val;
            }
            total = local_total;
            lastUpdate = System.currentTimeMillis();
        }
        int selected = random.nextInt(total);
        int sum = 0;
        for (int i = 0; i < targets.length; i++) {
            sum += loads[i];
            if (selected < sum) {
                return rets[i];
            }
        }
        return rets[rets.length-1];
    }
}
