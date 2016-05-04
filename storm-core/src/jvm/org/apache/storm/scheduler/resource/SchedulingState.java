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

package org.apache.storm.scheduler.resource;

import org.apache.storm.Config;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.Topologies;

import java.util.HashMap;
import java.util.Map;

/**
 * Class that holds the completely scheduling state of Resource Aware Scheduler
 */
public class SchedulingState {
    public final Map<String, User> userMap = new HashMap<String, User>();
    public final Cluster cluster;
    public final Topologies topologies;
    public final RAS_Nodes nodes;
    public final Map conf = new Config();

    public SchedulingState(Map<String, User> userMap, Cluster cluster, Topologies topologies, Map conf) {
        for (Map.Entry<String, User> userMapEntry : userMap.entrySet()) {
            String userId = userMapEntry.getKey();
            User user = userMapEntry.getValue();
            this.userMap.put(userId, new User(user));
        }
        this.cluster = new Cluster(cluster);
        this.topologies = new Topologies(topologies);
        this.nodes = new RAS_Nodes(this.cluster, this.topologies);
        this.conf.putAll(conf);
    }

    /**
     * copy constructor
     */
    public SchedulingState(SchedulingState src) {
        this(src.userMap, src.cluster, src.topologies, src.conf);
    }
}
