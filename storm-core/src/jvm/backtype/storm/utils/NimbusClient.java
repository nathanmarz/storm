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

import backtype.storm.generated.Nimbus;
import backtype.storm.nimbus.ILeaderElector;
import backtype.storm.nimbus.NimbusInfo;
import backtype.storm.security.auth.ThriftClient;
import clojure.lang.IFn;
import clojure.lang.PersistentArrayMap;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;

public class NimbusClient extends ThriftClient {
    private Nimbus.Client _client;
    private static final Logger LOG = LoggerFactory.getLogger(NimbusClient.class);

    public static NimbusClient getConfiguredClient(Map conf) {
        ILeaderElector zkLeaderElector = null;
        try {
            IFn zkLeaderElectorFn = Utils.loadClojureFn("backtype.storm.zookeeper", "zk-leader-elector");
            zkLeaderElector = (ILeaderElector) zkLeaderElectorFn.invoke(PersistentArrayMap.create(conf));
            NimbusInfo leaderInfo = zkLeaderElector.getLeader();
            String nimbusHost = leaderInfo.getHost();
            int nimbusPort = leaderInfo.getPort();
            return new NimbusClient(conf, nimbusHost, nimbusPort);
        } catch (TTransportException ex) {
            throw new RuntimeException(ex);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if(zkLeaderElector != null) {
                zkLeaderElector.close();
            }
        }
    }

    public NimbusClient(Map conf, String host, int port) throws TTransportException {
        this(conf, host, port, null);
    }

    public NimbusClient(Map conf, String host, int port, Integer timeout) throws TTransportException {
        super(conf, host, port, timeout);
        _client = new Nimbus.Client(_protocol);
    }

    public Nimbus.Client getClient() {
        return _client;
    }
}
