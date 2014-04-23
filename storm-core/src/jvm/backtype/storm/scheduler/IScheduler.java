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
package backtype.storm.scheduler;

import java.util.Map;


public interface IScheduler {
    
    void prepare(Map conf);
    
    /**
     * Set assignments for the topologies which needs scheduling. The new assignments is available 
     * through <code>cluster.getAssignments()</code>
     *
     *@param topologies all the topologies in the cluster, some of them need schedule. Topologies object here 
     *       only contain static information about topologies. Information like assignments, slots are all in
     *       the <code>cluster</code>object.
     *@param cluster the cluster these topologies are running in. <code>cluster</code> contains everything user
     *       need to develop a new scheduling logic. e.g. supervisors information, available slots, current 
     *       assignments for all the topologies etc. User can set the new assignment for topologies using
     *       <code>cluster.setAssignmentById</code>
     */
    void schedule(Topologies topologies, Cluster cluster);
}
