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
package backtype.storm;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;

import java.util.Map;


public interface ILocalCluster {
    void submitTopology(String topologyName, Map conf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException;
    void submitTopologyWithOpts(String topologyName, Map conf, StormTopology topology, SubmitOptions submitOpts) throws AlreadyAliveException, InvalidTopologyException;
    void killTopology(String topologyName) throws NotAliveException;
    void killTopologyWithOpts(String name, KillOptions options) throws NotAliveException;
    void activate(String topologyName) throws NotAliveException;
    void deactivate(String topologyName) throws NotAliveException;
    void rebalance(String name, RebalanceOptions options) throws NotAliveException;
    void shutdown();
    String getTopologyConf(String id);
    StormTopology getTopology(String id);
    ClusterSummary getClusterInfo();
    TopologyInfo getTopologyInfo(String id);
    Map getState();
}
