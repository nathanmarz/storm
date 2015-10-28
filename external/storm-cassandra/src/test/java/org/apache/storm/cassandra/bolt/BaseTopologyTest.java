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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.cassandra.bolt;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import backtype.storm.LocalCluster;

import org.junit.Rule;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class BaseTopologyTest {

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("schema.cql","weather"));

    protected void runLocalTopologyAndWait(TopologyBuilder builder) {
        LocalCluster cluster = new LocalCluster();
        StormTopology topology = builder.createTopology();
        Config config = getConfig();
        cluster.submitTopology("my-cassandra-topology", config, topology);

        Utils.sleep(TimeUnit.SECONDS.toMillis(15));

        cluster.killTopology("my-cassandra-topology");
        cluster.shutdown();
    }

    protected Config getConfig() {
        Config config = new Config();
        config.put("cassandra.keyspace", "weather");
        config.put("cassandra.nodes", "localhost");
        config.put("cassandra.port", "9142");
        return config;
    }
}
