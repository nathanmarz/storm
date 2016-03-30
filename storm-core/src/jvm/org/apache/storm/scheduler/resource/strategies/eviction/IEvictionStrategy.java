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

package org.apache.storm.scheduler.resource.strategies.eviction;

import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.SchedulingState;

public interface IEvictionStrategy {

    /**
     * Initialization
     */
    public void prepare(SchedulingState schedulingState);

    /**
     * This method when invoked should attempt to make space on the cluster so that the topology specified can be scheduled
     * @param td the topology to make space for
     * @return return true to indicate that space has been made for topology and try schedule topology td again.
     * Return false to inidcate that no space could be made for the topology on the cluster and the scheduler should give up
     * trying to schedule the topology for this round of scheduling.  This method will be invoked until the topology indicated
     * could be scheduled or the method returns false
     */
    public boolean makeSpaceForTopo(TopologyDetails td);


}
