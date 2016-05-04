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
package org.apache.storm.trident.drpc;

import org.apache.storm.Config;
import org.apache.storm.drpc.DRPCInvocationsClient;
import org.apache.storm.generated.DistributedRPCInvocations;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.utils.ServiceRegistry;
import org.apache.storm.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.json.simple.JSONValue;
import org.apache.storm.trident.drpc.ReturnResultsReducer.ReturnResultsState;
import org.apache.storm.trident.operation.MultiReducer;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentMultiReducerContext;
import org.apache.storm.trident.tuple.TridentTuple;


public class ReturnResultsReducer implements MultiReducer<ReturnResultsState> {
    public static class ReturnResultsState {
        List<TridentTuple> results = new ArrayList<>();
        String returnInfo;

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }
    boolean local;
    Map conf;
    Map<List, DRPCInvocationsClient> _clients = new HashMap<>();
    
    
    @Override
    public void prepare(Map conf, TridentMultiReducerContext context) {
        this.conf = conf;
        local = conf.get(Config.STORM_CLUSTER_MODE).equals("local");
    }

    @Override
    public ReturnResultsState init(TridentCollector collector) {
        return new ReturnResultsState();
    }

    @Override
    public void execute(ReturnResultsState state, int streamIndex, TridentTuple input, TridentCollector collector) {
        if(streamIndex==0) {
            state.returnInfo = input.getString(0);
        } else {
            state.results.add(input);
        }
    }

    @Override
    public void complete(ReturnResultsState state, TridentCollector collector) {
        // only one of the multireducers will receive the tuples
        if(state.returnInfo!=null) {
            String result = JSONValue.toJSONString(state.results);
            Map retMap = (Map) JSONValue.parse(state.returnInfo);
            final String host = (String) retMap.get("host");
            final int port = Utils.getInt(retMap.get("port"));
            String id = (String) retMap.get("id");
            DistributedRPCInvocations.Iface client;
            if(local) {
                client = (DistributedRPCInvocations.Iface) ServiceRegistry.getService(host);
            } else {
                List server = new ArrayList() {{
                    add(host);
                    add(port);
                }};

                if(!_clients.containsKey(server)) {
                    try {
                        _clients.put(server, new DRPCInvocationsClient(conf, host, port));
                    } catch (TTransportException ex) {
                        throw new RuntimeException(ex);
                    }
                }
                client = _clients.get(server);
            }

            try {
                client.result(id, result);
            } catch (AuthorizationException aze) {
                collector.reportError(aze);                
            } catch(TException e) {
                collector.reportError(e);
            }
        }
    }

    @Override
    public void cleanup() {
        for(DRPCInvocationsClient c: _clients.values()) {
            c.close();
        }
    }
    
}
