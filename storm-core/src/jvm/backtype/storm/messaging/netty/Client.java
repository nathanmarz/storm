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
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class Client implements IConnection {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private static final Timer TIMER = new Timer("netty-client-timer", true);

    private final int max_retries;
    private final long base_sleep_ms;
    private final long max_sleep_ms;
    private LinkedBlockingQueue<Object> message_queue; //entry should either be TaskMessage or ControlMessage
    private AtomicReference<Channel> channelRef;
    private final ClientBootstrap bootstrap;
    InetSocketAddress remote_addr;
    private AtomicInteger retries;
    private final Random random = new Random();
    private final ChannelFactory factory;
    private final int buffer_size;
    private final AtomicBoolean being_closed;
    private boolean wait_for_requests;

    @SuppressWarnings("rawtypes")
    Client(Map storm_conf, ChannelFactory factory, String host, int port) {
        this.factory = factory;
        message_queue = new LinkedBlockingQueue<Object>();
        retries = new AtomicInteger(0);
        channelRef = new AtomicReference<Channel>(null);
        being_closed = new AtomicBoolean(false);
        wait_for_requests = false;

        // Configure
        buffer_size = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        max_retries = Math.min(30, Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES)));
        base_sleep_ms = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
        max_sleep_ms = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));

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
        close_n_release();

        //reconnect only if it's not being closed
        if (being_closed.get()) return;

        final int tried_count = retries.incrementAndGet();
        if (tried_count <= max_retries) {
            long sleep = getSleepTimeMs();
            LOG.info("Waiting {} ms before trying connection to {}", sleep, remote_addr);
            TIMER.schedule(new TimerTask() {
                @Override
                public void run() { 
                    LOG.info("Reconnect ... [{}] to {}", tried_count, remote_addr);
                    bootstrap.connect(remote_addr);
                }}, sleep);
        } else {
            LOG.warn(remote_addr+" is not reachable. We will close this client.");
            close();
        }
    }

    /**
     * # of milliseconds to wait per exponential back-off policy
     */
    private long getSleepTimeMs()
    {
        if (retries.get() > 30) {
           return max_sleep_ms;
        }
        int backoff = 1 << retries.get();
        long sleepMs = base_sleep_ms * Math.max(1, random.nextInt(backoff));
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

            //resume delivery if it is waiting for requests
            tryDeliverMessages(true);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieve messages from queue, and delivery to server if any
     */
    synchronized void tryDeliverMessages(boolean only_if_waiting) throws InterruptedException {
        //just skip if delivery only if waiting, and we are not waiting currently
        if (only_if_waiting && !wait_for_requests)  return;

        //make sure that channel was not closed
        Channel channel = channelRef.get();
        if (channel == null)  return;
        if (!channel.isOpen()) {
            LOG.info("Channel to {} is no longer open.",remote_addr);
            //The channel is not open yet. Reconnect?
            reconnect();
            return;
        }

        final MessageBatch requests = tryTakeMessages();
        if (requests==null) {
            wait_for_requests = true;
            return;
        }

        //if channel is being closed and we have no outstanding messages,  let's close the channel
        if (requests.isEmpty() && being_closed.get()) {
            close_n_release();
            return;
        }

        //we are busily delivering messages, and will check queue upon response.
        //When send() is called by senders, we should not thus call tryDeliverMessages().
        wait_for_requests = false;

        //write request into socket channel
        ChannelFuture future = channel.write(requests);
        future.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future)
                    throws Exception {
                if (!future.isSuccess()) {
                    LOG.info("failed to send "+requests.size()+" requests to "+remote_addr, future.getCause());
                    reconnect();
                } else {
                    LOG.debug("{} request(s) sent", requests.size());

                    //Now that our requests have been sent, channel could be closed if needed
                    if (being_closed.get())
                        close_n_release();
                }
            }
        });
    }

    /**
     * Take all enqueued messages from queue
     * @return  batch of messages
     * @throws InterruptedException
     *
     * synchronized ... ensure that messages are delivered in the same order
     * as they are added into queue
     */
    private MessageBatch tryTakeMessages() throws InterruptedException {
        //1st message
        Object msg = message_queue.poll();
        if (msg == null) return null;

        MessageBatch batch = new MessageBatch(buffer_size);
        //we will discard any message after CLOSE
        if (msg == ControlMessage.CLOSE_MESSAGE) {
            LOG.info("Connection to {} is being closed", remote_addr);
            being_closed.set(true);
            return batch;
        }

        batch.add((TaskMessage)msg);
        while (!batch.isFull() && ((msg = message_queue.peek())!=null)) {
            //Is it a CLOSE message?
            if (msg == ControlMessage.CLOSE_MESSAGE) {
                message_queue.take();
                LOG.info("Connection to {} is being closed", remote_addr);
                being_closed.set(true);
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
    public void close() {
        //enqueue a CLOSE message so that shutdown() will be invoked
        try {
            message_queue.put(ControlMessage.CLOSE_MESSAGE);

            //resume delivery if it is waiting for requests
            tryDeliverMessages(true);
        } catch (InterruptedException e) {
            LOG.info("Interrupted Connection to {} is being closed", remote_addr);
            being_closed.set(true);
            close_n_release();
        }
    }

    /**
     * close_n_release() is invoked after all messages have been sent.
     */
    synchronized void close_n_release() {
        if (channelRef.get() != null) {
            channelRef.get().close();
            LOG.debug("channel {} closed",remote_addr);
            setChannel(null);
        }
    }

    public TaskMessage recv(int flags) {
        throw new RuntimeException("Client connection should not receive any messages");
    }

    void setChannel(Channel channel) {
        if (channel != null && channel.isOpen()) {
            //Assume the most recent connection attempt was successful.
            retries.set(0);
        }
        channelRef.set(channel);
        //reset retries
        if (channel != null)
            retries.set(0);
    }

}
