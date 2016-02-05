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
package org.apache.storm.scheduler;

import java.util.Map;
import java.util.Collection;


public interface ISupervisor {
    void prepare(Map stormConf, String schedulerLocalDir);
    // for mesos, this is {hostname}-{topologyid}
    /**
     * The id used for writing metadata into ZK.
     */
    String getSupervisorId();
    /**
     * The id used in assignments. This combined with confirmAssigned decides what
     * this supervisor is responsible for. The combination of this and getSupervisorId
     * allows Nimbus to assign to a single machine and have multiple supervisors
     * on that machine execute the assignment. This is important for achieving resource isolation.
     */
    String getAssignmentId();
    Object getMetadata();
    
    boolean confirmAssigned(int port);
    // calls this before actually killing the worker locally...
    // sends a "task finished" update
    void killedWorker(int port);
    void assigned(Collection<Integer> ports);
}
