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

package org.apache.storm.scheduler.resource.strategies.scheduling;

import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.SchedulingResult;
import org.apache.storm.scheduler.resource.SchedulingState;

/**
 * An interface to for implementing different scheduling strategies for the resource aware scheduling
 * In the future stategies will be pluggable
 */
public interface IStrategy {

    /**
     * initialize prior to scheduling
     */
    void prepare(SchedulingState schedulingState);

    /**
     * This method is invoked to calcuate a scheduling for topology td
     * @param td
     * @return returns a SchedulingResult object containing SchedulingStatus object to indicate whether scheduling is successful
     * The strategy must calculate a scheduling in the format of Map<WorkerSlot, Collection<ExecutorDetails>> where the key of
     * this map is the worker slot that the value (collection of executors) should be assigned to.
     * if a scheduling is calculated successfully, put the scheduling map in the SchedulingResult object.
     * PLEASE NOTE: Any other operations done on the cluster from a scheduling strategy will NOT persist or be realized.
     * The data structures passed in can be used in any way necessary to assist in calculating a scheduling, but will NOT actually change the state of the cluster.
     */
    SchedulingResult schedule(TopologyDetails td);
}
