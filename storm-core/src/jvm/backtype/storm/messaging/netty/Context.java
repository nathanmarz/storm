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
package backtype.storm.messaging.netty;

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.utils.Utils;

public class Context implements IContext {
    private static final Logger LOG = LoggerFactory.getLogger(Context.class);
        
    @SuppressWarnings("rawtypes")
    private Map storm_conf;
    private Map<String, IConnection> connections;
    private NioClientSocketChannelFactory clientChannelFactory;
    
    private ScheduledExecutorService clientScheduleService;
    private final int MAX_CLIENT_SCHEDULER_THREAD_POOL_SIZE = 10;

    /**
     * initialization per Storm configuration 
     */
    @SuppressWarnings("rawtypes")
    public void prepare(Map storm_conf) {
        this.storm_conf = storm_conf;
        connections = new HashMap<String, IConnection>();

        //each context will have a single client channel factory
        int maxWorkers = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS));
		ThreadFactory bossFactory = new NettyRenameThreadFactory("client" + "-boss");
        ThreadFactory workerFactory = new NettyRenameThreadFactory("client" + "-worker");
        if (maxWorkers > 0) {
            clientChannelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(bossFactory),
                    Executors.newCachedThreadPool(workerFactory), maxWorkers);
        } else {
            clientChannelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(bossFactory),
                    Executors.newCachedThreadPool(workerFactory));
        }
        
        int otherWorkers = Utils.getInt(storm_conf.get(Config.TOPOLOGY_WORKERS), 1) - 1;
        int poolSize = Math.min(Math.max(1, otherWorkers), MAX_CLIENT_SCHEDULER_THREAD_POOL_SIZE);
        clientScheduleService = Executors.newScheduledThreadPool(poolSize, new NettyRenameThreadFactory("client-schedule-service"));
    }

    /**
     * establish a server with a binding port
     */
    public synchronized IConnection bind(String storm_id, int port) {
        IConnection server = new Server(storm_conf, port);
        connections.put(key(storm_id, port), server);
        return server;
    }

    /**
     * establish a connection to a remote server
     */
    public synchronized IConnection connect(String storm_id, String host, int port) {
        IConnection connection = connections.get(key(host,port));
        if(connection !=null)
        {
            return connection;
        }
        IConnection client =  new Client(storm_conf, clientChannelFactory, 
                clientScheduleService, host, port, this);
        connections.put(key(host, port), client);
        return client;
    }

    synchronized void removeClient(String host, int port) {
        connections.remove(key(host, port));
    }

    /**
     * terminate this context
     */
    public synchronized void term() {
        clientScheduleService.shutdown();        
        
        for (IConnection conn : connections.values()) {
            conn.close();
        }
        
        try {
            clientScheduleService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Error when shutting down client scheduler", e);
        }
        
        connections = null;

        //we need to release resources associated with client channel factory
        clientChannelFactory.releaseExternalResources();

    }

    private String key(String host, int port) {
        return String.format("%s:%d", host, port);
    }
}
