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
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class Client implements IConnection {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private static final String PREFIX = "Netty-Client-";
    private final int max_retries;
    private final long base_sleep_ms;
    private final long max_sleep_ms;
    private AtomicReference<Channel> channelRef;
    private final ClientBootstrap bootstrap;
    private InetSocketAddress remote_addr;
    
    private final Random random = new Random();
    private final ChannelFactory factory;
    private final int buffer_size;
    private boolean closing;

    private int messageBatchSize;
    
    private AtomicLong pendings;

    MessageBatch messageBatch = null;
    private AtomicLong flushCheckTimer;
    private int flushCheckInterval;
    private ScheduledExecutorService scheduler;

    @SuppressWarnings("rawtypes")
    Client(Map storm_conf, ChannelFactory factory, 
            ScheduledExecutorService scheduler, String host, int port) {
        this.factory = factory;
        this.scheduler = scheduler;
        channelRef = new AtomicReference<Channel>(null);
        closing = false;
        pendings = new AtomicLong(0);
        flushCheckTimer = new AtomicLong(Long.MAX_VALUE);

        // Configure
        buffer_size = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        max_retries = Math.min(30, Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES)));
        base_sleep_ms = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
        max_sleep_ms = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));

        this.messageBatchSize = Utils.getInt(storm_conf.get(Config.STORM_NETTY_MESSAGE_BATCH_SIZE), 262144);
        
        flushCheckInterval = Utils.getInt(storm_conf.get(Config.STORM_NETTY_FLUSH_CHECK_INTERVAL_MS), 10); // default 10 ms

        LOG.info("New Netty Client, connect to " + host + ", " + port
                + ", config: " + ", buffer_size: " + buffer_size);

        bootstrap = new ClientBootstrap(factory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("sendBufferSize", buffer_size);
        bootstrap.setOption("keepAlive", true);

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new StormClientPipelineFactory(this));

        // Start the connection attempt.
        remote_addr = new InetSocketAddress(host, port);
        
        // setup the connection asyncly now
        scheduler.execute(new Runnable() {
            @Override
            public void run() {   
                connect();
            }
        });
        
        Runnable flusher = new Runnable() {
            @Override
            public void run() {

                while(!closing) {
                    long flushCheckTime = flushCheckTimer.get();
                    long now = System.currentTimeMillis();
                    if (now > flushCheckTime) {
                        Channel channel = channelRef.get();
                        if (null != channel && channel.isWritable()) {
                            flush(channel);
                        }
                    }
                }
                
            }
        };
        
        long initialDelay = Math.min(30L * 1000, max_sleep_ms * max_retries); //max wait for 30s
        scheduler.scheduleWithFixedDelay(flusher, initialDelay, flushCheckInterval, TimeUnit.MILLISECONDS);
    }

    /**
     * We will retry connection with exponential back-off policy
     */
    private synchronized void connect() {
        try {
            if (channelRef.get() != null) {
                return;
            }
            
            Channel channel = null;

            int tried = 0;
            while (tried <= max_retries) {

                LOG.info("Reconnect started for {}... [{}]", name(), tried);
                LOG.debug("connection started...");

                ChannelFuture future = bootstrap.connect(remote_addr);
                future.awaitUninterruptibly();
                Channel current = future.getChannel();
                if (!future.isSuccess()) {
                    if (null != current) {
                        current.close();
                    }
                } else {
                    channel = current;
                    break;
                }
                Thread.sleep(getSleepTimeMs(tried));
                tried++;  
            }
            if (null != channel) {
                LOG.info("connection established to a remote host " + name() + ", " + channel.toString());
                channelRef.set(channel);
            } else {
                close();
                throw new RuntimeException("Remote address is not reachable. We will close this client " + name());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("connection failed " + name(), e);
        }
    }

    /**
     * # of milliseconds to wait per exponential back-off policy
     */
    private long getSleepTimeMs(int retries) {
        if (retries > 30) {
           return max_sleep_ms;
        }
        int backoff = 1 << retries;
        long sleepMs = base_sleep_ms * Math.max(1, random.nextInt(backoff));
        if ( sleepMs > max_sleep_ms )
            sleepMs = max_sleep_ms;
        return sleepMs;
    }

    /**
     * Enqueue task messages to be sent to server
     */
    synchronized public void send(Iterator<TaskMessage> msgs) {

        // throw exception if the client is being closed
        if (closing) {
            throw new RuntimeException("Client is being closed, and does not take requests any more");
        }
        
        if (null == msgs || !msgs.hasNext()) {
            return;
        }

        Channel channel = channelRef.get();
        if (null == channel) {
            connect();
            channel = channelRef.get();
        }

        while (msgs.hasNext()) {
            TaskMessage message = msgs.next();
            if (null == messageBatch) {
                messageBatch = new MessageBatch(messageBatchSize);
            }

            messageBatch.add(message);
            if (messageBatch.isFull()) {
                MessageBatch toBeFlushed = messageBatch;
                flushRequest(channel, toBeFlushed);
                messageBatch = null;
            }
        }

        if (null != messageBatch && !messageBatch.isEmpty()) {
            if (channel.isWritable()) {
                flushCheckTimer.set(Long.MAX_VALUE);
                
                // Flush as fast as we can to reduce the latency
                MessageBatch toBeFlushed = messageBatch;
                messageBatch = null;
                flushRequest(channel, toBeFlushed);
                
            } else {
                // when channel is NOT writable, it means the internal netty buffer is full. 
                // In this case, we can try to buffer up more incoming messages.
                flushCheckTimer.set(System.currentTimeMillis() + flushCheckInterval);
            }
        }

    }

    public String name() {
        if (null != remote_addr) {
            return PREFIX + remote_addr.toString();
        }
        return "";
    }

    private synchronized void flush(Channel channel) {
        if (!closing) {
            if (null != messageBatch && !messageBatch.isEmpty()) {
                MessageBatch toBeFlushed = messageBatch;
                flushCheckTimer.set(Long.MAX_VALUE);
                flushRequest(channel, toBeFlushed);
                messageBatch = null;
            }
        }
    }
    
    /**
     * gracefully close this client.
     * 
     * We will send all existing requests, and then invoke close_n_release()
     * method
     */
    public synchronized void close() {
        if (!closing) {
            closing = true;
            LOG.info("Closing Netty Client " + name());
            
            if (null != messageBatch && !messageBatch.isEmpty()) {
                MessageBatch toBeFlushed = messageBatch;
                Channel channel = channelRef.get();
                if (channel != null) {
                    flushRequest(channel, toBeFlushed);
                }
                messageBatch = null;
            }
        
            //wait for pendings to exit
            final long timeoutMilliSeconds = 600 * 1000; //600 seconds
            final long start = System.currentTimeMillis();
            
            LOG.info("Waiting for pending batchs to be sent with "+ name() + "..., timeout: {}ms, pendings: {}", timeoutMilliSeconds, pendings.get());
            
            while(pendings.get() != 0) {
                try {
                    long delta = System.currentTimeMillis() - start;
                    if (delta > timeoutMilliSeconds) {
                        LOG.error("Timeout when sending pending batchs with {}..., there are still {} pending batchs not sent", name(), pendings.get());
                        break;
                    }
                    Thread.sleep(1000); //sleep 1s
                } catch (InterruptedException e) {
                    break;
                } 
            }
            
            close_n_release();
        }
    }

    /**
     * close_n_release() is invoked after all messages have been sent.
     */
    private void close_n_release() {
        if (channelRef.get() != null) {
            channelRef.get().close();
            LOG.debug("channel {} closed",remote_addr);
        }
    }

    @Override
    public Iterator<TaskMessage> recv(int flags, int clientId) {
        throw new RuntimeException("Client connection should not receive any messages");
    }

    @Override
    public void send(int taskId, byte[] payload) {
        TaskMessage msg = new TaskMessage(taskId, payload);
        List<TaskMessage> wrapper = new ArrayList<TaskMessage>(1);
        wrapper.add(msg);
        send(wrapper.iterator());
    }

    private void flushRequest(Channel channel, final MessageBatch requests) {
        if (requests == null)
            return;

        pendings.incrementAndGet();
        ChannelFuture future = channel.write(requests);
        future.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future)
                    throws Exception {

                pendings.decrementAndGet();
                if (!future.isSuccess()) {
                    LOG.info(
                            "failed to send requests to " + remote_addr.toString() + ": ", future.getCause());

                    Channel channel = future.getChannel();

                    if (null != channel) {
                        channel.close();
                        channelRef.compareAndSet(channel, null);
                    }
                } else {
                    LOG.debug("{} request(s) sent", requests.size());
                }
            }
        });
    }
}