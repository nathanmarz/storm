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
package backtype.storm.metric.api;

import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import java.util.Collection;
import java.util.Map;

public interface IMetricsConsumer {
    public static class TaskInfo {
        public TaskInfo() {}
        public TaskInfo(String srcWorkerHost, int srcWorkerPort, String srcComponentId, int srcTaskId, long timestamp, int updateIntervalSecs) {
            this.srcWorkerHost = srcWorkerHost;
            this.srcWorkerPort = srcWorkerPort;
            this.srcComponentId = srcComponentId; 
            this.srcTaskId = srcTaskId; 
            this.timestamp = timestamp;
            this.updateIntervalSecs = updateIntervalSecs; 
        }
        public String srcWorkerHost;
        public int srcWorkerPort;
        public String srcComponentId; 
        public int srcTaskId; 
        public long timestamp;
        public int updateIntervalSecs; 
    }
    public static class DataPoint {
        public DataPoint() {}
        public DataPoint(String name, Object value) {
            this.name = name;
            this.value = value;
        }
        @Override
        public String toString() {
            return "[" + name + " = " + value + "]";
        }
        public String name; 
        public Object value;
    }

    void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter);
    void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints);
    void cleanup();
}