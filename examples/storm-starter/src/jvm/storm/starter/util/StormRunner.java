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
package storm.starter.util;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;

public final class StormRunner {

  private static final int MILLIS_IN_SEC = 1000;

  private StormRunner() {
  }

  public static void runTopologyLocally(StormTopology topology, String topologyName, Config conf, int runtimeInSeconds)
      throws InterruptedException {
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(topologyName, conf, topology);
    Thread.sleep((long) runtimeInSeconds * MILLIS_IN_SEC);
    cluster.killTopology(topologyName);
    cluster.shutdown();
  }

  public static void runTopologyRemotely(StormTopology topology, String topologyName, Config conf)
      throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
    StormSubmitter.submitTopology(topologyName, conf, topology);
  }
}
