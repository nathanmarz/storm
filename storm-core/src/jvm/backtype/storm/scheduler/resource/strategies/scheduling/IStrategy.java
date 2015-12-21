/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package backtype.storm.scheduler.resource.strategies.scheduling;

import java.util.Map;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.resource.RAS_Nodes;
import backtype.storm.scheduler.resource.SchedulingResult;
import backtype.storm.scheduler.resource.User;

/**
 * An interface to for implementing different scheduling strategies for the resource aware scheduling
 * In the future stategies will be pluggable
 */
public interface IStrategy {

    /**
     * initialize prior to scheduling
     */
    public void prepare(Topologies topologies, Cluster cluster, Map<String, User> userMap, RAS_Nodes nodes);

    /**
     * This method is invoked to calcuate a scheduling for topology td
     * @param td
     * @return returns a SchedulingResult object containing SchedulingStatus object to indicate whether scheduling is successful
     * The strategy must calculate a scheduling in the format of Map<WorkerSlot, Collection<ExecutorDetails>> where the key of
     * this map is the worker slot that the value (collection of executors) should be assigned to.
     * if a scheduling is calculated successfully, put the scheduling map in the SchedulingResult object.
     */
    public SchedulingResult schedule(TopologyDetails td);
}
