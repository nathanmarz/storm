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
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class Client implements IConnection {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private final int max_retries;
    private final int base_sleep_ms;
    private final int max_sleep_ms;
    private LinkedBlockingQueue<Object> message_queue; //entry should either be TaskMessage or ControlMessage
    private AtomicReference<Channel> channelRef;
    private final ClientBootstrap bootstrap;
    private InetSocketAddress remote_addr;
    private AtomicInteger retries;
    private final Random random = new Random();
    private final ChannelFactory factory;
    private final int buffer_size;
    private final AtomicBoolean being_closed;

    @SuppressWarnings("rawtypes")
    Client(Map storm_conf, String host, int port) {
        message_queue = new LinkedBlockingQueue<Object>();
        retries = new AtomicInteger(0);
        channelRef = new AtomicReference<Channel>(null);
        being_closed = new AtomicBoolean(false);

        // Configure
        buffer_size = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        max_retries = Math.min(30, Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES)));
        base_sleep_ms = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
        max_sleep_ms = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));
        int maxWorkers = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS));

        if (maxWorkers > 0) {
            factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), maxWorkers);
        } else {
            factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
        }
        bootstrap = new ClientBootstrap(factory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("sendBufferSize", buffer_size);
        bootstrap.setOption("keepAlive", true);

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new StormClientPipelineFactory(this));

        // Start the connection attempt.
        remote_addr = new InetSocketAddress(host, port);
        bootstrap.connect(remote_addr);
    }

    /**
     * We will retry connection with exponential back-off policy
     */
    void reconnect() {
        try {
            int tried_count = retries.incrementAndGet();
            if (tried_count <= max_retries) {
                Thread.sleep(getSleepTimeMs());
                LOG.info("Reconnect ... [{}]", tried_count);
                bootstrap.connect(remote_addr);
                LOG.debug("connection started...");
            } else {
                LOG.warn("Remote address is not reachable. We will close this client.");
                close();
            }
        } catch (InterruptedException e) {
            LOG.warn("connection failed", e);
        }
    }

    /**
     * # of milliseconds to wait per exponential back-off policy
     */
    private int getSleepTimeMs()
    {
        int backoff = 1 << retries.get();
        int sleepMs = base_sleep_ms * Math.max(1, random.nextInt(backoff));
        if ( sleepMs > max_sleep_ms )
            sleepMs = max_sleep_ms;
        return sleepMs;
    }

    /**
     * Enqueue a task message to be sent to server
     */
    public void send(int task, byte[] message) {
        //throw exception if the client is being closed
        if (being_closed.get()) {
            throw new RuntimeException("Client is being closed, and does not take requests any more");
        }

        try {
            message_queue.put(new TaskMessage(task, message));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Take all enqueued messages from queue
     * @return
     * @throws InterruptedException
     */
    MessageBatch takeMessages()  throws InterruptedException {
        //1st message
        MessageBatch batch = new MessageBatch(buffer_size);
        Object msg = message_queue.take();
        batch.add(msg);

        //we will discard any message after CLOSE
        if (msg==ControlMessage.CLOSE_MESSAGE)
            return batch;

        while (!batch.isFull()) {
            //peek the next message
            msg = message_queue.peek();
            //no more messages
            if (msg == null) break;

            //we will discard any message after CLOSE
            if (msg==ControlMessage.CLOSE_MESSAGE) {
                message_queue.take();
                batch.add(msg);
                break;
            }

            //try to add this msg into batch
            if (!batch.tryAdd((TaskMessage) msg))
                break;

            //remove this message
            message_queue.take();
        }

        return batch;
    }

    /**
     * gracefully close this client.
     *
     * We will send all existing requests, and then invoke close_n_release() method
     */
    public synchronized void close() {
        if (!being_closed.get()) {
            //enqueue a CLOSE message so that shutdown() will be invoked
            try {
                message_queue.put(ControlMessage.CLOSE_MESSAGE);
                being_closed.set(true);
            } catch (InterruptedException e) {
                close_n_release();
            }
        }
    }

    /**
     * close_n_release() is invoked after all messages have been sent.
     */
    void  close_n_release() {
        if (channelRef.get() != null)
            channelRef.get().close().awaitUninterruptibly();

        //we need to release resources
        new Thread(new Runnable() {
            @Override
            public void run() {
                factory.releaseExternalResources();
            }}).start();
    }

    public TaskMessage recv(int flags) {
        throw new RuntimeException("Client connection should not receive any messages");
    }

    void setChannel(Channel channel) {
        channelRef.set(channel);
        //reset retries
        if (channel != null)
            retries.set(0);
    }

}




