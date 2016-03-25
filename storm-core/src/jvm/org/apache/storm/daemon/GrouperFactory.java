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
package org.apache.storm.daemon;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.apache.storm.Config;
import org.apache.storm.Thrift;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.grouping.LoadAwareCustomStreamGrouping;
import org.apache.storm.grouping.LoadAwareShuffleGrouping;
import org.apache.storm.grouping.LoadMapping;
import org.apache.storm.grouping.ShuffleGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.TupleUtils;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class GrouperFactory {

    public static LoadAwareCustomStreamGrouping mkGrouper(WorkerTopologyContext context, String componentId, String streamId, Fields outFields,
                                    Grouping thriftGrouping,
                                    List<Integer> unsortedTargetTasks,
                                    Map topoConf) {
        List<Integer> targetTasks = Ordering.natural().sortedCopy(unsortedTargetTasks);
        final boolean isNotLoadAware = (null != topoConf.get(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING) && (boolean) topoConf
            .get(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING));
        CustomStreamGrouping result = null;
        switch (Thrift.groupingType(thriftGrouping)) {
            case FIELDS:
                if (Thrift.isGlobalGrouping(thriftGrouping)) {
                    result = new GlobalGrouper();
                } else {
                    result = new FieldsGrouper(outFields, thriftGrouping);
                }
                break;
            case SHUFFLE:
                if (isNotLoadAware) {
                    result = new ShuffleGrouping();
                } else {
                    result = new LoadAwareShuffleGrouping();
                }
                break;
            case ALL:
                result = new AllGrouper();
                break;
            case LOCAL_OR_SHUFFLE:
                // Prefer local tasks as target tasks if possible
                Set<Integer> sameTasks = Sets.intersection(Sets.newHashSet(targetTasks), Sets.newHashSet(context.getThisWorkerTasks()));
                targetTasks = (sameTasks.isEmpty()) ? targetTasks : new ArrayList<>(sameTasks);
                if (isNotLoadAware) {
                    result = new ShuffleGrouping();
                } else {
                    result = new LoadAwareShuffleGrouping();
                }
                break;
            case NONE:
                result = new NoneGrouper();
                break;
            case CUSTOM_OBJECT:
                result = (CustomStreamGrouping) Thrift.instantiateJavaObject(thriftGrouping.get_custom_object());
                break;
            case CUSTOM_SERIALIZED:
                result = Utils.javaDeserialize(thriftGrouping.get_custom_serialized(), CustomStreamGrouping.class);
                break;
            case DIRECT:
                result = DIRECT;
                break;
            default:
                result = null;
                break;
        }

        if (null != result) {
            result.prepare(context, new GlobalStreamId(componentId, streamId), targetTasks);
        }

        if (result instanceof LoadAwareCustomStreamGrouping) {
            return (LoadAwareCustomStreamGrouping) result;
        } else {
            return new BasicLoadAwareCustomStreamGrouping (result);
        }
    }


    /**
     * A bridge between CustomStreamGrouping and LoadAwareCustomStreamGrouping
     */
    public static class BasicLoadAwareCustomStreamGrouping implements LoadAwareCustomStreamGrouping {

        private final CustomStreamGrouping customStreamGrouping;

        public BasicLoadAwareCustomStreamGrouping(CustomStreamGrouping customStreamGrouping) {
            this.customStreamGrouping = customStreamGrouping;
        }

        @Override
        public List<Integer> chooseTasks(int taskId, List<Object> values, LoadMapping load) {
            return customStreamGrouping.chooseTasks(taskId, values);
        }

        @Override
        public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
            customStreamGrouping.prepare(context, stream, targetTasks);
        }

        @Override
        public List<Integer> chooseTasks(int taskId, List<Object> values) {
            return customStreamGrouping.chooseTasks(taskId, values);
        }
    }

    public static class FieldsGrouper implements CustomStreamGrouping {

        private Fields outFields;
        private List<Integer> targetTasks;
        private Fields groupFields;
        private int numTasks;

        public FieldsGrouper(Fields outFields, Grouping thriftGrouping) {
            this.outFields = outFields;
            this.groupFields = new Fields(Thrift.fieldGrouping(thriftGrouping));

        }

        @Override
        public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
            this.targetTasks = targetTasks;
            this.numTasks = targetTasks.size();
        }

        @Override
        public List<Integer> chooseTasks(int taskId, List<Object> values) {
            int targetTaskIndex = Math.abs(TupleUtils.listHashCode(outFields.select(groupFields, values))) % numTasks;
            return Collections.singletonList(targetTasks.get(targetTaskIndex));
        }
    }

    public static class GlobalGrouper implements CustomStreamGrouping {

        private List<Integer> targetTasks;

        public GlobalGrouper() {
        }

        @Override
        public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
            this.targetTasks = targetTasks;
        }

        @Override
        public List<Integer> chooseTasks(int taskId, List<Object> values) {
            if (targetTasks.isEmpty()) {
                return null;
            }
            // It's possible for target to have multiple tasks if it reads multiple sources
            return Collections.singletonList(targetTasks.get(0));
        }
    }

    public static class NoneGrouper implements CustomStreamGrouping {

        private List<Integer> targetTasks;
        private int numTasks;
        private final Random random;

        public NoneGrouper() {
            random = new Random();
        }

        @Override
        public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
            this.targetTasks = targetTasks;
            this.numTasks = targetTasks.size();
        }

        @Override
        public List<Integer> chooseTasks(int taskId, List<Object> values) {
            int index = random.nextInt(numTasks);
            return Collections.singletonList(targetTasks.get(index));
        }
    }

    public static class AllGrouper implements CustomStreamGrouping {

        private List<Integer> targetTasks;

        @Override
        public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
            this.targetTasks = targetTasks;
        }

        @Override
        public List<Integer> chooseTasks(int taskId, List<Object> values) {
            return targetTasks;
        }
    }

    // A no-op grouper
    public static final LoadAwareCustomStreamGrouping DIRECT = new LoadAwareCustomStreamGrouping() {
        @Override
        public List<Integer> chooseTasks(int taskId, List<Object> values, LoadMapping load) {
            return null;
        }

        @Override
        public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {

        }

        @Override
        public List<Integer> chooseTasks(int taskId, List<Object> values) {
            return null;
        }

    };

}
