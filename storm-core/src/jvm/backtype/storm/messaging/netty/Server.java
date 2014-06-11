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

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.Utils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

class Server implements IConnection {
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);
    @SuppressWarnings("rawtypes")
    Map storm_conf;
    int port;
    
    // Create multiple queues for incoming messages. The size equals the number of receiver threads.
    // For message which is sent to same task, it will be stored in the same queue to preserve the message order.
    private LinkedBlockingQueue<ArrayList<TaskMessage>>[] message_queue;
    
    volatile ChannelGroup allChannels = new DefaultChannelGroup("storm-server");
    final ChannelFactory factory;
    final ServerBootstrap bootstrap;
    
    private int queueCount;
    HashMap<Integer, Integer> taskToQueueId = null;
    int roundRobinQueueId;
	
    boolean closing = false;
    List<TaskMessage> closeMessage = Arrays.asList(new TaskMessage(-1, null));
    
    
    @SuppressWarnings("rawtypes")
    Server(Map storm_conf, int port) {
        this.storm_conf = storm_conf;
        this.port = port;
        
        queueCount = Utils.getInt(storm_conf.get(Config.WORKER_RECEIVER_THREAD_COUNT), 1);
        roundRobinQueueId = 0;
        taskToQueueId = new HashMap<Integer, Integer>();
    
        message_queue = new LinkedBlockingQueue[queueCount];
        for (int i = 0; i < queueCount; i++) {
            message_queue[i] = new LinkedBlockingQueue<ArrayList<TaskMessage>>();
        }
        
        // Configure the server.
        int buffer_size = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        int maxWorkers = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS));

        ThreadFactory bossFactory = new NettyRenameThreadFactory(name() + "-boss");
        ThreadFactory workerFactory = new NettyRenameThreadFactory(name() + "-worker");
        
        if (maxWorkers > 0) {
            factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(bossFactory), 
                Executors.newCachedThreadPool(workerFactory), maxWorkers);
        } else {
            factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(bossFactory), 
                Executors.newCachedThreadPool(workerFactory));
        }
        
        LOG.info("Create Netty Server " + name() + ", buffer_size: " + buffer_size + ", maxWorkers: " + maxWorkers);
        
        bootstrap = new ServerBootstrap(factory);
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.receiveBufferSize", buffer_size);
        bootstrap.setOption("child.keepAlive", true);

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new StormServerPipelineFactory(this));

        // Bind and start to accept incoming connections.
        Channel channel = bootstrap.bind(new InetSocketAddress(port));
        allChannels.add(channel);
    }
    
    private ArrayList<TaskMessage>[] groupMessages(List<TaskMessage> msgs) {
      ArrayList<TaskMessage> messageGroups[] = new ArrayList[queueCount];
      
      for (int i = 0; i < msgs.size(); i++) {
        TaskMessage message = msgs.get(i);
        int task = message.task();
        
        if (task == -1) {
          closing = true;
          return null;
        }
        
        Integer queueId = getMessageQueueId(task);
        
        if (null == messageGroups[queueId]) {
          messageGroups[queueId] = new ArrayList<TaskMessage>();
        }
        messageGroups[queueId].add(message);
      }
      return messageGroups;
    }
    
    private Integer getMessageQueueId(int task) {
      // try to construct the map from taskId -> queueId in round robin manner.
      
      Integer queueId = taskToQueueId.get(task);
      if (null == queueId) {
        synchronized(taskToQueueId) {
          //assgin task to queue in round-robin manner
          if (null == taskToQueueId.get(task)) {
            queueId = roundRobinQueueId++;
            
            taskToQueueId.put(task, queueId);
            if (roundRobinQueueId == queueCount) {
              roundRobinQueueId = 0;
            }
          }
        }
      }
      return queueId;
    }

    /**
     * enqueue a received message 
     * @param message
     * @throws InterruptedException
     */
    protected void enqueue(List<TaskMessage> msgs) throws InterruptedException {
      
      if (null == msgs || msgs.size() == 0 || closing) {
        return;
      }
      
      ArrayList<TaskMessage> messageGroups[] = groupMessages(msgs);
      
      if (null == messageGroups || closing) {
        return;
      }
      
      for (int receiverId = 0; receiverId < messageGroups.length; receiverId++) {
        ArrayList<TaskMessage> msgGroup = messageGroups[receiverId];
        if (null != msgGroup) {
          message_queue[receiverId].put(msgGroup);
        }
      }
    }
    
    public Iterator<TaskMessage> recv(int flags, int receiverId)  {
      if (closing) {
        return closeMessage.iterator();
      }
      
      ArrayList<TaskMessage> ret = null; 
      int queueId = receiverId % queueCount;
      if ((flags & 0x01) == 0x01) { 
            //non-blocking
            ret = message_queue[queueId].poll();
        } else {
            try {
                ArrayList<TaskMessage> request = message_queue[queueId].take();
                LOG.debug("request to be processed: {}", request);
                ret = request;
            } catch (InterruptedException e) {
                LOG.info("exception within msg receiving", e);
                ret = null;
            }
        }
      
      if (null != ret) {
        return ret.iterator();
      }
      return null;
    }
   
    /**
     * register a newly created channel
     * @param channel
     */
    protected void addChannel(Channel channel) {
        allChannels.add(channel);
    }
    
    /**
     * close a channel
     * @param channel
     */
    protected void closeChannel(Channel channel) {
        channel.close().awaitUninterruptibly();
        allChannels.remove(channel);
    }

    /**
     * close all channels, and release resources
     */
    public synchronized void close() {
        if (allChannels != null) {
            allChannels.close().awaitUninterruptibly();
            factory.releaseExternalResources();
            allChannels = null;
        }
    }

    public void send(int task, byte[] message) {
        throw new RuntimeException("Server connection should not send any messages");
    }
    
    public void send(Iterator<TaskMessage> msgs) {
      throw new RuntimeException("Server connection should not send any messages");
    }
	
    public String name() {
      return "Netty-server-localhost-" + port;
    }
}
