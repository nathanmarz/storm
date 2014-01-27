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
import backtype.storm.ILocalDRPC;
import backtype.storm.generated.DRPCRequest;
import backtype.storm.generated.DistributedRPCInvocations;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.ServiceRegistry;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.thrift.TException;
import org.json.simple.JSONValue;

public class DRPCSpout extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(DRPCSpout.class);
    
    SpoutOutputCollector _collector;
    List<DRPCInvocationsClient> _clients = new ArrayList<DRPCInvocationsClient>();
    String _function;
    String _local_drpc_id = null;
    
    private static class DRPCMessageId {
        String id;
        int index;
        
        public DRPCMessageId(String id, int index) {
            this.id = id;
            this.index = index;
        }
    }
    
    
    public DRPCSpout(String function) {
        _function = function;
    }

    public DRPCSpout(String function, ILocalDRPC drpc) {
        _function = function;
        _local_drpc_id = drpc.getServiceId();
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        if(_local_drpc_id==null) {
            int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
            int index = context.getThisTaskIndex();

            int port = Utils.getInt(conf.get(Config.DRPC_INVOCATIONS_PORT));
            List<String> servers = (List<String>) conf.get(Config.DRPC_SERVERS);
            if(servers == null || servers.isEmpty()) {
                throw new RuntimeException("No DRPC servers configured for topology");   
            }
            if(numTasks < servers.size()) {
                for(String s: servers) {
                    _clients.add(new DRPCInvocationsClient(s, port));
                }
            } else {
                int i = index % servers.size();
                _clients.add(new DRPCInvocationsClient(servers.get(i), port));
            }
        }
        
    }

    @Override
    public void close() {
        for(DRPCInvocationsClient client: _clients) {
            client.close();
        }
    }

    @Override
    public void nextTuple() {
        boolean gotRequest = false;
        if(_local_drpc_id==null) {
            for(int i=0; i<_clients.size(); i++) {
                DRPCInvocationsClient client = _clients.get(i);
                try {
                    DRPCRequest req = client.fetchRequest(_function);
                    if(req.get_request_id().length() > 0) {
                        Map returnInfo = new HashMap();
                        returnInfo.put("id", req.get_request_id());
                        returnInfo.put("host", client.getHost());
                        returnInfo.put("port", client.getPort());
                        gotRequest = true;
                        _collector.emit(new Values(req.get_func_args(), JSONValue.toJSONString(returnInfo)), new DRPCMessageId(req.get_request_id(), i));
                        break;
                    }
                } catch (Exception e) {
                    LOG.error("Failed to fetch DRPC result from DRPC server", e);
                }
            }
        } else {
            DistributedRPCInvocations.Iface drpc = (DistributedRPCInvocations.Iface) ServiceRegistry.getService(_local_drpc_id);
            if(drpc!=null) { // can happen during shutdown of drpc while topology is still up
                try {
                    DRPCRequest req = drpc.fetchRequest(_function);
                    if(req.get_request_id().length() > 0) {
                        Map returnInfo = new HashMap();
                        returnInfo.put("id", req.get_request_id());
                        returnInfo.put("host", _local_drpc_id);
                        returnInfo.put("port", 0);
                        gotRequest = true;
                        _collector.emit(new Values(req.get_func_args(), JSONValue.toJSONString(returnInfo)), new DRPCMessageId(req.get_request_id(), 0));
                    }
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if(!gotRequest) {
            Utils.sleep(1);
        }
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
        DRPCMessageId did = (DRPCMessageId) msgId;
        DistributedRPCInvocations.Iface client;
        
        if(_local_drpc_id == null) {
            client = _clients.get(did.index);
        } else {
            client = (DistributedRPCInvocations.Iface) ServiceRegistry.getService(_local_drpc_id);
        }
        try {
            client.failRequest(did.id);
        } catch (TException e) {
            LOG.error("Failed to fail request", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("args", "return-info"));
    }    
}
