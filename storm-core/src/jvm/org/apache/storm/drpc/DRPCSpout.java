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
package org.apache.storm.drpc;

import org.apache.storm.Config;
import org.apache.storm.ILocalDRPC;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.generated.DistributedRPCInvocations;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.ExtendedThreadPoolExecutor;
import org.apache.storm.utils.ServiceRegistry;
import org.apache.storm.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.thrift.TException;
import org.json.simple.JSONValue;

public class DRPCSpout extends BaseRichSpout {
    //ANY CHANGE TO THIS CODE MUST BE SERIALIZABLE COMPATIBLE OR THERE WILL BE PROBLEMS
    static final long serialVersionUID = 2387848310969237877L;

    public static final Logger LOG = LoggerFactory.getLogger(DRPCSpout.class);
    
    SpoutOutputCollector _collector;
    List<DRPCInvocationsClient> _clients = new ArrayList<>();
    transient LinkedList<Future<Void>> _futures = null;
    transient ExecutorService _backround = null;
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

    public String get_function() {
        return _function;
    }

    private class Adder implements Callable<Void> {
        private String server;
        private int port;
        private Map conf;

        public Adder(String server, int port, Map conf) {
            this.server = server;
            this.port = port;
            this.conf = conf;
        }

        @Override
        public Void call() throws Exception {
            DRPCInvocationsClient c = new DRPCInvocationsClient(conf, server, port);
            synchronized (_clients) {
                _clients.add(c);
            }
            return null;
        }
    }

    private void reconnect(final DRPCInvocationsClient c) {
        _futures.add(_backround.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                c.reconnectClient();
                return null;
            }
        }));
    }

    private void checkFutures() {
        Iterator<Future<Void>> i = _futures.iterator();
        while (i.hasNext()) {
            Future<Void> f = i.next();
            if (f.isDone()) {
                i.remove();
            }
            try {
                f.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
 
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        if(_local_drpc_id==null) {
            _backround = new ExtendedThreadPoolExecutor(0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>());
            _futures = new LinkedList<>();

            int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
            int index = context.getThisTaskIndex();

            int port = Utils.getInt(conf.get(Config.DRPC_INVOCATIONS_PORT));
            List<String> servers = (List<String>) conf.get(Config.DRPC_SERVERS);
            if(servers == null || servers.isEmpty()) {
                throw new RuntimeException("No DRPC servers configured for topology");   
            }
            
            if (numTasks < servers.size()) {
                for (String s: servers) {
                    _futures.add(_backround.submit(new Adder(s, port, conf)));
                }
            } else {        
                int i = index % servers.size();
                _futures.add(_backround.submit(new Adder(servers.get(i), port, conf)));
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
            int size;
            synchronized (_clients) {
                size = _clients.size(); //This will only ever grow, so no need to worry about falling off the end
            }
            for(int i=0; i<size; i++) {
                DRPCInvocationsClient client;
                synchronized (_clients) {
                    client = _clients.get(i);
                }
                if (!client.isConnected()) {
                    LOG.warn("DRPCInvocationsClient [{}:{}] is not connected.", client.getHost(), client.getPort());
                    reconnect(client);
                    continue;
                }
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
                } catch (AuthorizationException aze) {
                    reconnect(client);
                    LOG.error("Not authorized to fetch DRPC result from DRPC server", aze);
                } catch (TException e) {
                    reconnect(client);
                    LOG.error("Failed to fetch DRPC result from DRPC server", e);
                } catch (Exception e) {
                    LOG.error("Failed to fetch DRPC result from DRPC server", e);
                }
            }
            checkFutures();
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
                } catch (AuthorizationException aze) {
                    throw new RuntimeException(aze);
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
        } catch (AuthorizationException aze) {
            LOG.error("Not authorized to failREquest from DRPC server", aze);
        } catch (TException e) {
            LOG.error("Failed to fail request", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("args", "return-info"));
    }    
}
