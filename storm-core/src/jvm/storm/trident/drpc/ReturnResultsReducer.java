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
package storm.trident.drpc;

import backtype.storm.Config;
import backtype.storm.drpc.DRPCInvocationsClient;
import backtype.storm.generated.DistributedRPCInvocations;
import backtype.storm.utils.ServiceRegistry;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.thrift.TException;
import org.json.simple.JSONValue;
import storm.trident.drpc.ReturnResultsReducer.ReturnResultsState;
import storm.trident.operation.MultiReducer;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentMultiReducerContext;
import storm.trident.tuple.TridentTuple;


public class ReturnResultsReducer implements MultiReducer<ReturnResultsState> {
    public static class ReturnResultsState {
        List<TridentTuple> results = new ArrayList<TridentTuple>();
        String returnInfo;

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }
    boolean local;

    Map<List, DRPCInvocationsClient> _clients = new HashMap<List, DRPCInvocationsClient>();
    
    
    @Override
    public void prepare(Map conf, TridentMultiReducerContext context) {
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
                    _clients.put(server, new DRPCInvocationsClient(host, port));
                }
                client = _clients.get(server);
            }

            try {
                client.result(id, result);
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
