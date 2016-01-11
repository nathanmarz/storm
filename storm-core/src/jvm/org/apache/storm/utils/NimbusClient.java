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
package org.apache.storm.utils;


import org.apache.storm.Config;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.security.auth.ThriftClient;
import org.apache.storm.security.auth.ThriftConnectionType;
import com.google.common.collect.Lists;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class NimbusClient extends ThriftClient {
    private Nimbus.Client _client;
    private static final Logger LOG = LoggerFactory.getLogger(NimbusClient.class);


    public static NimbusClient getConfiguredClient(Map conf) {
        return getConfiguredClientAs(conf, null);
    }

    public static NimbusClient getConfiguredClientAs(Map conf, String asUser) {
        if (conf.containsKey(Config.STORM_DO_AS_USER)) {
            if (asUser != null && !asUser.isEmpty()) {
                LOG.warn("You have specified a doAsUser as param {} and a doAsParam as config, config will take precedence."
                        , asUser, conf.get(Config.STORM_DO_AS_USER));
            }
            asUser = (String) conf.get(Config.STORM_DO_AS_USER);
        }

        List<String> seeds;
        if(conf.containsKey(Config.NIMBUS_HOST)) {
            LOG.warn("Using deprecated config {} for backward compatibility. Please update your storm.yaml so it only has config {}",
                    Config.NIMBUS_HOST, Config.NIMBUS_SEEDS);
            seeds = Lists.newArrayList(conf.get(Config.NIMBUS_HOST).toString());
        } else {
            seeds = (List<String>) conf.get(Config.NIMBUS_SEEDS);
        }

        for (String host : seeds) {
            int port = Integer.parseInt(conf.get(Config.NIMBUS_THRIFT_PORT).toString());
            ClusterSummary clusterInfo;
            try {
                NimbusClient client = new NimbusClient(conf, host, port, null, asUser);
                clusterInfo = client.getClient().getClusterInfo();
            } catch (Exception e) {
                LOG.warn("Ignoring exception while trying to get leader nimbus info from " + host
                        + ". will retry with a different seed host.", e);
                continue;
            }
            List<NimbusSummary> nimbuses = clusterInfo.get_nimbuses();
            if (nimbuses != null) {
                for (NimbusSummary nimbusSummary : nimbuses) {
                    if (nimbusSummary.is_isLeader()) {
                        try {
                            return new NimbusClient(conf, nimbusSummary.get_host(), nimbusSummary.get_port(), null, asUser);
                        } catch (TTransportException e) {
                            String leaderNimbus = nimbusSummary.get_host() + ":" + nimbusSummary.get_port();
                            throw new RuntimeException("Failed to create a nimbus client for the leader " + leaderNimbus, e);
                        }
                    }
                }
                throw new NimbusLeaderNotFoundException(
                        "Found nimbuses " + nimbuses + " none of which is elected as leader, please try " +
                        "again after some time.");
            }
        }
        throw new NimbusLeaderNotFoundException(
                "Could not find leader nimbus from seed hosts " + seeds + ". " +
                "Did you specify a valid list of nimbus hosts for config " +
                        Config.NIMBUS_SEEDS + "?");
    }

    public NimbusClient(Map conf, String host, int port) throws TTransportException {
        this(conf, host, port, null, null);
    }

    public NimbusClient(Map conf, String host, int port, Integer timeout) throws TTransportException {
        super(conf, ThriftConnectionType.NIMBUS, host, port, timeout, null);
        _client = new Nimbus.Client(_protocol);
    }

    public NimbusClient(Map conf, String host, Integer port, Integer timeout, String asUser) throws TTransportException {
        super(conf, ThriftConnectionType.NIMBUS, host, port, timeout, asUser);
        _client = new Nimbus.Client(_protocol);
    }

    public NimbusClient(Map conf, String host) throws TTransportException {
        super(conf, ThriftConnectionType.NIMBUS, host, null, null, null);
        _client = new Nimbus.Client(_protocol);
    }

    public Nimbus.Client getClient() {
        return _client;
    }
}
