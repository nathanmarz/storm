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
package org.apache.storm.trident.partition;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class IdentityGrouping implements CustomStreamGrouping {
    final Map<Integer, List<Integer>> _precomputed = new HashMap<>();
    
    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> tasks) {
        List<Integer> sourceTasks = new ArrayList<>(context.getComponentTasks(stream.get_componentId()));
        Collections.sort(sourceTasks);
        if(sourceTasks.size()!=tasks.size()) {
            throw new RuntimeException("Can only do an identity grouping when source and target have same number of tasks");
        }
        tasks = new ArrayList<>(tasks);
        Collections.sort(tasks);
        for(int i=0; i<sourceTasks.size(); i++) {
            int s = sourceTasks.get(i);
            int t = tasks.get(i);
            _precomputed.put(s, Arrays.asList(t));
        }
    }

    @Override
    public List<Integer> chooseTasks(int task, List<Object> values) {
        List<Integer> ret = _precomputed.get(task);
        if(ret==null) {
            throw new RuntimeException("Tuple emitted by task that's not part of this component. Should be impossible");
        }
        return ret;
    }
    
}
