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
package backtype.storm.utils;

import backtype.storm.Config;
import backtype.storm.security.auth.ThriftClient;
import backtype.storm.security.auth.ThriftConnectionType;
import backtype.storm.generated.Nimbus;
import java.util.Map;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NimbusClient extends ThriftClient {
    private Nimbus.Client _client;
    private static final Logger LOG = LoggerFactory.getLogger(NimbusClient.class);


    public static NimbusClient getConfiguredClient(Map conf) {
        try {
            String nimbusHost = (String) conf.get(Config.NIMBUS_HOST);
            return new NimbusClient(conf, nimbusHost);
        } catch (TTransportException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static NimbusClient getConfiguredClientAs(Map conf, String asUser) {
        try {
            if(conf.containsKey(Config.STORM_DO_AS_USER)) {
                if(asUser != null && !asUser.isEmpty()) {
                    LOG.warn("You have specified a doAsUser as param {} and a doAsParam as config, config will take precedence."
                            , asUser, conf.get(Config.STORM_DO_AS_USER));
                }
                asUser = (String) conf.get(Config.STORM_DO_AS_USER);
            }
            String nimbusHost = (String) conf.get(Config.NIMBUS_HOST);
            return new NimbusClient(conf, nimbusHost, null, null, asUser);
        } catch (TTransportException ex) {
            throw new RuntimeException(ex);
        }
    }

    public NimbusClient(Map conf, String host, int port) throws TTransportException {
        this(conf, host, port, null);
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
