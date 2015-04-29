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
package backtype.storm.drpc;

import backtype.storm.Config;
import backtype.storm.generated.DistributedRPCInvocations;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.ServiceRegistry;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.json.simple.JSONValue;


public class ReturnResults extends BaseRichBolt {
    //ANY CHANGE TO THIS CODE MUST BE SERIALIZABLE COMPATIBLE OR THERE WILL BE PROBLEMS
    static final long serialVersionUID = -774882142710631591L;

    public static final Logger LOG = LoggerFactory.getLogger(ReturnResults.class);
    OutputCollector _collector;
    boolean local;
    Map _conf; 
    Map<List, DRPCInvocationsClient> _clients = new HashMap<List, DRPCInvocationsClient>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _conf = stormConf;
        _collector = collector;
        local = stormConf.get(Config.STORM_CLUSTER_MODE).equals("local");
    }

    @Override
    public void execute(Tuple input) {
        String result = (String) input.getValue(0);
        String returnInfo = (String) input.getValue(1);
        if(returnInfo!=null) {
            Map retMap = (Map) JSONValue.parse(returnInfo);
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
                        _clients.put(server, new DRPCInvocationsClient(_conf, host, port));
                    } catch (TTransportException ex) {
                        throw new RuntimeException(ex);
                    }
                }
                client = _clients.get(server);
            }
 
            try {
                client.result(id, result);
                _collector.ack(input);
            } catch (AuthorizationException aze) {
                LOG.error("Not authorized to return results to DRPC server", aze);
                _collector.fail(input);
                if (client instanceof DRPCInvocationsClient) {
                    try {
                        LOG.info("reconnecting... ");
                        ((DRPCInvocationsClient)client).reconnectClient(); //Blocking call
                    } catch (TException e2) {
                        throw new RuntimeException(e2);
                    }
                }
            } catch(TException e) {
                LOG.error("Failed to return results to DRPC server", e);
                _collector.fail(input);
                if (client instanceof DRPCInvocationsClient) {
                    try {
                        LOG.info("reconnecting... ");
                        ((DRPCInvocationsClient)client).reconnectClient(); //Blocking call
                    } catch (TException e2) {
                        throw new RuntimeException(e2);
                    }
                }
            }
        }
    }    

    @Override
    public void cleanup() {
        for(DRPCInvocationsClient c: _clients.values()) {
            c.close();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
