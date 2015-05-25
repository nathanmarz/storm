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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.*;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.ConnectionWithStatus;
import backtype.storm.metric.api.IStatefulObject;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.StormBoundedExponentialBackoffRetry;
import backtype.storm.utils.Utils;

/**
 * A Netty client for sending task messages to a remote destination (Netty server).
 *
 * Implementation details:
 *
 * - Sending messages, i.e. writing to the channel, is performed asynchronously.
 * - Messages are sent in batches to optimize for network throughput at the expense of network latency.  The message
 *   batch size is configurable.
 * - Connecting and reconnecting are performed asynchronously.
 *     - Note: The current implementation drops any messages that are being enqueued for sending if the connection to
 *       the remote destination is currently unavailable.
 * - A background flusher thread is run in the background.  It will, at fixed intervals, check for any pending messages
 *   (i.e. messages buffered in memory) and flush them to the remote destination iff background flushing is currently
 *   enabled.
 */
public class Client extends ConnectionWithStatus implements IStatefulObject {

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private static final String PREFIX = "Netty-Client-";
    private static final long NO_DELAY_MS = 0L;
    private static final long MINIMUM_INITIAL_DELAY_MS = 30000L;
    private static final long PENDING_MESSAGES_FLUSH_TIMEOUT_MS = 600000L;
    private static final long PENDING_MESSAGES_FLUSH_INTERVAL_MS = 1000L;
    private static final long DISTANT_FUTURE_TIME_MS = Long.MAX_VALUE;

    private final StormBoundedExponentialBackoffRetry retryPolicy;
    private final ClientBootstrap bootstrap;
    private final InetSocketAddress dstAddress;
    protected final String dstAddressPrefixedName;

    /**
     * The channel used for all write operations from this client to the remote destination.
     */
    private final AtomicReference<Channel> channelRef = new AtomicReference<Channel>(null);


    /**
     * Maximum number of reconnection attempts we will perform after a disconnect before giving up.
     */
    private final int maxReconnectionAttempts;

    /**
     * Total number of connection attempts.
     */
    private final AtomicInteger totalConnectionAttempts = new AtomicInteger(0);

    /**
     * Number of connection attempts since the last disconnect.
     */
    private final AtomicInteger connectionAttempts = new AtomicInteger(0);

    /**
     * Number of messages successfully sent to the remote destination.
     */
    private final AtomicInteger messagesSent = new AtomicInteger(0);

    /**
     * Number of messages that could not be sent to the remote destination.
     */
    private final AtomicInteger messagesLost = new AtomicInteger(0);

    /**
     * Number of messages buffered in memory.
     */
    private final AtomicLong pendingMessages = new AtomicLong(0);

    /**
     * This flag is set to true if and only if a client instance is being closed.
     */
    private volatile boolean closing = false;

    /**
     * When set to true, then the background flusher thread will flush any pending messages on its next run.
     */
    private final AtomicBoolean backgroundFlushingEnabled = new AtomicBoolean(false);

    /**
     * The absolute time (in ms) when the next background flush should be performed.
     *
     * Note: The flush operation will only be performed if backgroundFlushingEnabled is true, too.
     */
    private final AtomicLong nextBackgroundFlushTimeMs = new AtomicLong(DISTANT_FUTURE_TIME_MS);

    /**
     * The time interval (in ms) at which the background flusher thread will be run to check for any pending messages
     * to be flushed.
     */
    private final int flushCheckIntervalMs;

    /**
     * How many messages should be batched together before sending them to the remote destination.
     *
     * Messages are batched to optimize network throughput at the expense of latency.
     */
    private final int messageBatchSize;

    private MessageBatch messageBatch = null;
    private final ListeningScheduledExecutorService scheduler;
    protected final Map stormConf;
    private Context context;

    @SuppressWarnings("rawtypes")
    Client(Map stormConf, ChannelFactory factory, ScheduledExecutorService scheduler, String host, int port, Context context) {
        this.context = context;
        closing = false;
        this.stormConf = stormConf;
        this.scheduler =  MoreExecutors.listeningDecorator(scheduler);
        int bufferSize = Utils.getInt(stormConf.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        LOG.info("creating Netty Client, connecting to {}:{}, bufferSize: {}", host, port, bufferSize);
        messageBatchSize = Utils.getInt(stormConf.get(Config.STORM_NETTY_MESSAGE_BATCH_SIZE), 262144);
        flushCheckIntervalMs = Utils.getInt(stormConf.get(Config.STORM_NETTY_FLUSH_CHECK_INTERVAL_MS), 10);

        maxReconnectionAttempts = Utils.getInt(stormConf.get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES));
        int minWaitMs = Utils.getInt(stormConf.get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
        int maxWaitMs = Utils.getInt(stormConf.get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));
        retryPolicy = new StormBoundedExponentialBackoffRetry(minWaitMs, maxWaitMs, maxReconnectionAttempts);

        // Initiate connection to remote destination
        bootstrap = createClientBootstrap(factory, bufferSize);
        dstAddress = new InetSocketAddress(host, port);
        dstAddressPrefixedName = prefixedName(dstAddress);
        connect(NO_DELAY_MS);

        // Launch background flushing thread
        pauseBackgroundFlushing();
        long initialDelayMs = Math.min(MINIMUM_INITIAL_DELAY_MS, maxWaitMs * maxReconnectionAttempts);
        scheduler.scheduleWithFixedDelay(createBackgroundFlusher(), initialDelayMs, flushCheckIntervalMs,
            TimeUnit.MILLISECONDS);
    }

    private ClientBootstrap createClientBootstrap(ChannelFactory factory, int bufferSize) {
        ClientBootstrap bootstrap = new ClientBootstrap(factory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("sendBufferSize", bufferSize);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setPipelineFactory(new StormClientPipelineFactory(this));
        return bootstrap;
    }

    private String prefixedName(InetSocketAddress dstAddress) {
        if (null != dstAddress) {
            return PREFIX + dstAddress.toString();
        }
        return "";
    }

    private Runnable createBackgroundFlusher() {
        return new Runnable() {
            @Override
            public void run() {
                if(!closing && backgroundFlushingEnabled.get() && nowMillis() > nextBackgroundFlushTimeMs.get()) {
                    LOG.debug("flushing {} pending messages to {} in background", messageBatch.size(),
                        dstAddressPrefixedName);
                    flushPendingMessages();
                }
            }
        };
    }

    private void pauseBackgroundFlushing() {
        backgroundFlushingEnabled.set(false);
    }

    private void resumeBackgroundFlushing() {
        backgroundFlushingEnabled.set(true);
    }

    private synchronized void flushPendingMessages() {
        Channel channel = channelRef.get();
        if (containsMessages(messageBatch)) {
            if (connectionEstablished(channel)) {
                if (channel.isWritable()) {
                    pauseBackgroundFlushing();
                    MessageBatch toBeFlushed = messageBatch;
                    flushMessages(channel, toBeFlushed);
                    messageBatch = null;
                }
                else if (closing) {
                    // Ensure background flushing is enabled so that we definitely have a chance to re-try the flush
                    // operation in case the client is being gracefully closed (where we have a brief time window where
                    // the client will wait for pending messages to be sent).
                    resumeBackgroundFlushing();
                }
            }
            else {
                closeChannelAndReconnect(channel);
            }
        }
    }

    private long nowMillis() {
        return System.currentTimeMillis();
    }

    /**
     * We will retry connection with exponential back-off policy
     */
    private synchronized void connect(long delayMs) {
        try {
            if (closing) {
                LOG.info("not connecting to {} because this client is being closed", dstAddressPrefixedName);
                return;
            }

            if (connectionEstablished(channelRef.get())) {
                LOG.info("not connecting to {} because the connection is already established", dstAddressPrefixedName);
                return;
            }

            connectionAttempts.getAndIncrement();
            if (reconnectingAllowed()) {
                totalConnectionAttempts.getAndIncrement();
                LOG.info("connection attempt {} to {} scheduled to run in {} ms", connectionAttempts.get(),
                    dstAddressPrefixedName, delayMs);
                ListenableFuture<Channel> channelFuture = scheduler.schedule(
                    new Connector(dstAddress, connectionAttempts.get()), delayMs, TimeUnit.MILLISECONDS);
                Futures.addCallback(channelFuture, new FutureCallback<Channel>() {
                    @Override public void onSuccess(Channel result) {
                        if (connectionEstablished(result)) {
                            setChannel(result);
                            LOG.info("connection established to {}", dstAddressPrefixedName);
                            connectionAttempts.set(0);
                        }
                        else {
                            reconnectAgain(new RuntimeException("Returned channel was actually not established"));
                        }
                    }

                    @Override public void onFailure(Throwable t) {
                        reconnectAgain(t);
                    }

                    private void reconnectAgain(Throwable t) {
                        String baseMsg = String.format("connection attempt %s to %s failed", connectionAttempts,
                            dstAddressPrefixedName);
                        String failureMsg = (t == null)? baseMsg : baseMsg + ": " + t.toString();
                        LOG.error(failureMsg);
                        long nextDelayMs = retryPolicy.getSleepTimeMs(connectionAttempts.get(), 0);
                        connect(nextDelayMs);
                    }
                });
            }
            else {
                close();
                throw new RuntimeException("Giving up to connect to " + dstAddressPrefixedName + " after " +
                    connectionAttempts + " failed attempts");
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to connect to " + dstAddressPrefixedName, e);
        }
    }

    private void setChannel(Channel channel) {
        channelRef.set(channel);
    }

    private boolean reconnectingAllowed() {
        return !closing && connectionAttempts.get() <= (maxReconnectionAttempts + 1);
    }

    private boolean connectionEstablished(Channel channel) {
        // Because we are using TCP (which is a connection-oriented transport unlike UDP), a connection is only fully
        // established iff the channel is connected.  That is, a TCP-based channel must be in the CONNECTED state before
        // anything can be read or written to the channel.
        //
        // See:
        // - http://netty.io/3.9/api/org/jboss/netty/channel/ChannelEvent.html
        // - http://stackoverflow.com/questions/13356622/what-are-the-netty-channel-state-transitions
        return channel != null && channel.isConnected();
    }

    /**
     * Note:  Storm will check via this method whether a worker can be activated safely during the initial startup of a
     * topology.  The worker will only be activated once all of the its connections are ready.
     */
    @Override
    public Status status() {
        if (closing) {
            return Status.Closed;
        }
        else if (!connectionEstablished(channelRef.get())) {
            return Status.Connecting;
        }
        else {
            return Status.Ready;
        }
    }

    /**
     * Receiving messages is not supported by a client.
     *
     * @throws java.lang.UnsupportedOperationException whenever this method is being called.
     */
    @Override
    public Iterator<TaskMessage> recv(int flags, int clientId) {
        throw new UnsupportedOperationException("Client connection should not receive any messages");
    }

    @Override
    public void send(int taskId, byte[] payload) {
        TaskMessage msg = new TaskMessage(taskId, payload);
        List<TaskMessage> wrapper = new ArrayList<TaskMessage>(1);
        wrapper.add(msg);
        send(wrapper.iterator());
    }

    /**
     * Enqueue task messages to be sent to the remote destination (cf. `host` and `port`).
     */
    @Override
    public synchronized void send(Iterator<TaskMessage> msgs) {
        if (closing) {
            int numMessages = iteratorSize(msgs);
            LOG.error("discarding {} messages because the Netty client to {} is being closed", numMessages,
                dstAddressPrefixedName);
            return;
        }

        if (!hasMessages(msgs)) {
            return;
        }

        Channel channel = channelRef.get();
        if (!connectionEstablished(channel)) {
            // Closing the channel and reconnecting should be done before handling the messages.
            closeChannelAndReconnect(channel);
            handleMessagesWhenConnectionIsUnavailable(msgs);
            return;
        }

        // Collect messages into batches (to optimize network throughput), then flush them.
        while (msgs.hasNext()) {
            TaskMessage message = msgs.next();
            if (messageBatch == null) {
                messageBatch = new MessageBatch(messageBatchSize);
            }

            messageBatch.add(message);
            if (messageBatch.isFull()) {
                MessageBatch toBeFlushed = messageBatch;
                flushMessages(channel, toBeFlushed);
                messageBatch = null;
            }
        }

        // Handle any remaining messages in case the "last" batch was not full.
        if (containsMessages(messageBatch)) {
            if (connectionEstablished(channel) && channel.isWritable()) {
                // We can write to the channel, so we flush the remaining messages immediately to minimize latency.
                pauseBackgroundFlushing();
                MessageBatch toBeFlushed = messageBatch;
                messageBatch = null;
                flushMessages(channel, toBeFlushed);
            }
            else {
                // We cannot write to the channel, which means Netty's internal write buffer is full.
                // In this case, we buffer the remaining messages and wait for the next messages to arrive.
                //
                // Background:
                // Netty 3.x maintains an internal write buffer with a high water mark for each channel (default: 64K).
                // This represents the amount of data waiting to be flushed to operating system buffers.  If the
                // outstanding data exceeds this value then the channel is set to non-writable.  When this happens, a
                // INTEREST_CHANGED channel event is triggered.  Netty sets the channel to writable again once the data
                // has been flushed to the system buffers.
                //
                // See http://stackoverflow.com/questions/14049260
                resumeBackgroundFlushing();
                nextBackgroundFlushTimeMs.set(nowMillis() + flushCheckIntervalMs);
            }
        }

    }

    private boolean hasMessages(Iterator<TaskMessage> msgs) {
        return msgs != null && msgs.hasNext();
    }

    /**
     * We will drop pending messages and let at-least-once message replay kick in.
     *
     * Another option would be to buffer the messages in memory.  But this option has the risk of causing OOM errors,
     * especially for topologies that disable message acking because we don't know whether the connection recovery will
     * succeed  or not, and how long the recovery will take.
     */
    private void handleMessagesWhenConnectionIsUnavailable(Iterator<TaskMessage> msgs) {
        LOG.error("connection to {} is unavailable", dstAddressPrefixedName);
        dropMessages(msgs);
    }

    private void dropMessages(Iterator<TaskMessage> msgs) {
        // We consume the iterator by traversing and thus "emptying" it.
        int msgCount = iteratorSize(msgs);
        messagesLost.getAndAdd(msgCount);
        LOG.error("dropping {} message(s) destined for {}", msgCount, dstAddressPrefixedName);
    }

    private int iteratorSize(Iterator<TaskMessage> msgs) {
        int size = 0;
        if (msgs != null) {
            while (msgs.hasNext()) {
                size++;
                msgs.next();
            }
        }
        return size;
    }

    /**
     * Asynchronously writes the message batch to the channel.
     *
     * If the write operation fails, then we will close the channel and trigger a reconnect.
     */
    private synchronized void flushMessages(Channel channel, MessageBatch batch) {
        if (!containsMessages(batch)) {
            return;
        }

        final int numMessages = batch.size();
        pendingMessages.getAndAdd(numMessages);
        LOG.debug("writing {} messages to channel {}", batch.size(), channel.toString());
        ChannelFuture future = channel.write(batch);
        future.addListener(new ChannelFutureListener() {

            public void operationComplete(ChannelFuture future) throws Exception {
                pendingMessages.getAndAdd(0 - numMessages);
                if (future.isSuccess()) {
                    LOG.debug("sent {} messages to {}", numMessages, dstAddressPrefixedName);
                    messagesSent.getAndAdd(numMessages);
                }
                else {
                    LOG.error("failed to send {} messages to {}: {}", numMessages, dstAddressPrefixedName,
                        future.getCause());
                    closeChannelAndReconnect(future.getChannel());
                    messagesLost.getAndAdd(numMessages);
                }
            }

        });
    }

    private synchronized void closeChannelAndReconnect(Channel channel) {
        if (channel != null) {
            LOG.info("closing channel {} to {}", channel.toString(), dstAddressPrefixedName);
            channel.close();
            if (channelRef.compareAndSet(channel, null)) {
                LOG.info("triggering reconnect to {}", dstAddressPrefixedName);
                connect(NO_DELAY_MS);
            }
        }
    }

    private boolean containsMessages(MessageBatch batch) {
        return batch != null && !batch.isEmpty();
    }

    /**
     * Gracefully close this client.
     *
     * We will attempt to send any pending messages (i.e. messages currently buffered in memory) before closing the
     * client.
     */
    @Override
    public void close() {
        if (!closing) {
            LOG.info("closing Netty Client {}", dstAddressPrefixedName);
            context.removeClient(dstAddress.getHostName(),dstAddress.getPort());
            context = null;
            // Set closing to true to prevent any further reconnection attempts.
            closing = true;
            flushPendingMessages();
            waitForPendingMessagesToBeSent();
            closeChannel();
        }
    }

    private synchronized void waitForPendingMessagesToBeSent() {
        LOG.info("waiting up to {} ms to send {} pending messages to {}",
            PENDING_MESSAGES_FLUSH_TIMEOUT_MS, pendingMessages.get(), dstAddressPrefixedName);
        long totalPendingMsgs = pendingMessages.get();
        long startMs = nowMillis();
        while (pendingMessages.get() != 0) {
            try {
                long deltaMs = nowMillis() - startMs;
                if (deltaMs > PENDING_MESSAGES_FLUSH_TIMEOUT_MS) {
                    LOG.error("failed to send all pending messages to {} within timeout, {} of {} messages were not " +
                        "sent", dstAddressPrefixedName, pendingMessages.get(), totalPendingMsgs);
                    break;
                }
                Thread.sleep(PENDING_MESSAGES_FLUSH_INTERVAL_MS);
            }
            catch (InterruptedException e) {
                break;
            }
        }

    }

    private synchronized void closeChannel() {
        if (channelRef.get() != null) {
            channelRef.get().close();
            LOG.debug("channel to {} closed", dstAddressPrefixedName);
        }
    }

    @Override
    public Object getState() {
        LOG.info("Getting metrics for client connection to {}", dstAddressPrefixedName);
        HashMap<String, Object> ret = new HashMap<String, Object>();
        ret.put("reconnects", totalConnectionAttempts.getAndSet(0));
        ret.put("sent", messagesSent.getAndSet(0));
        ret.put("pending", pendingMessages.get());
        ret.put("lostOnSend", messagesLost.getAndSet(0));
        ret.put("dest", dstAddress.toString());
        String src = srcAddressName();
        if (src != null) {
            ret.put("src", src);
        }
        return ret;
    }

    private String srcAddressName() {
        String name = null;
        Channel c = channelRef.get();
        if (c != null) {
            SocketAddress address = c.getLocalAddress();
            if (address != null) {
                name = address.toString();
            }
        }
        return name;
    }

    @Override public String toString() {
        return String.format("Netty client for connecting to %s", dstAddressPrefixedName);
    }

    /**
     * Asynchronously establishes a Netty connection to the remote address, returning a Netty Channel on success.
     */
    private class Connector implements Callable<Channel> {

        private final InetSocketAddress address;
        private final int connectionAttempt;

        public Connector(InetSocketAddress address, int connectionAttempt) {
            this.address = address;
            if (connectionAttempt < 1) {
                throw new IllegalArgumentException("connection attempt must be >= 1 (you provided " +
                    connectionAttempt + ")");
            }
            this.connectionAttempt = connectionAttempt;
        }

        @Override public Channel call() throws Exception {
            LOG.debug("connecting to {} [attempt {}]", address.toString(), connectionAttempt);
            Channel channel = null;
            ChannelFuture future = bootstrap.connect(address);
            future.awaitUninterruptibly();
            Channel current = future.getChannel();

            if (future.isSuccess() && connectionEstablished(current)) {
                channel = current;
                LOG.debug("successfully connected to {}, {} [attempt {}]", address.toString(), channel.toString(),
                    connectionAttempt);
            }
            else {
                LOG.debug("failed to connect to {} [attempt {}]", address.toString(), connectionAttempt);
                if (current != null) {
                    current.close();
                }
            }
            return channel;
        }
    }

}
