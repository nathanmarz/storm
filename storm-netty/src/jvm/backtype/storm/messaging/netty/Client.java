package backtype.storm.messaging.netty;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.Utils;

class Client implements IConnection {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private final int max_retries; 
    private final int base_sleep_ms; 
    private final int max_sleep_ms; 
    private LinkedBlockingQueue<TaskMessage> message_queue;
    private AtomicReference<Channel> channelRef;
    private final ClientBootstrap bootstrap;
    private InetSocketAddress remote_addr;
    private AtomicInteger retries; 
    private final Random random = new Random();
    private final ChannelFactory factory;
    private AtomicBoolean ready_to_release_resource;
    
    @SuppressWarnings("rawtypes")
    Client(Map storm_conf, String host, int port) {
        message_queue = new LinkedBlockingQueue<TaskMessage>();
        retries = new AtomicInteger(0);
        channelRef = new AtomicReference<Channel>(null);
        ready_to_release_resource = new AtomicBoolean(false);

        // Configure 
        int buffer_size = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        max_retries = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES));
        base_sleep_ms = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
        max_sleep_ms = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));
        
        factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());                   
        bootstrap = new ClientBootstrap(factory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("sendBufferSize", buffer_size);
        bootstrap.setOption("keepAlive", true);

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new StormClientPipelineFactory(this, storm_conf));

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
            if (tried_count < max_retries) {
                Thread.sleep(getSleepTimeMs());
                LOG.info("Reconnect ... " + " ["+tried_count+"]");   
                bootstrap.connect(remote_addr);
                LOG.debug("connection started...");
            } else {
                LOG.warn("Remote address is not reachable. We will close this client.");
                close();
            }
        } catch (InterruptedException e) {
            LOG.info("connection failed", e);
        } 
    }

    /**
     * # of milliseconds to wait per exponential back-off policy
     */
    private int getSleepTimeMs()
    {
        int sleepMs = base_sleep_ms * Math.max(1, random.nextInt(1 << retries.get()));
        if ( sleepMs > max_sleep_ms )
            sleepMs = max_sleep_ms;
        return sleepMs;
    }

    /**
     * Enqueue a task message to be sent to server 
     */
    public void send(int task, byte[] message) {
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
    ArrayList<TaskMessage> takeMessages()  throws InterruptedException {
        ArrayList<TaskMessage> requests = new ArrayList<TaskMessage>();
        requests.add(message_queue.take());
        for (TaskMessage msg = message_queue.poll(); msg!=null;  msg = message_queue.poll())
            requests.add(msg); 
        return requests;
    }

    /**
     * gracefully close this client.
     * 
     * We will send all existing requests, and then invoke close_n_release() method
     */
    public void close() {
        //enqueue a SHUTDOWN message so that shutdown() will be invoked 
        try {
            message_queue.put(Util.CLOSE_MESSAGE);
        } catch (InterruptedException e) {
            close_n_release();
        }
        
        //schedule a timer to release resources once channel is closed
        final Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (ready_to_release_resource.get()) {
                    LOG.info("client resource released");
                    factory.releaseExternalResources();
                    timer.cancel();
                }
            }
        }, 0, 10);
    }

    /**
     * close_n_release() is invoked after all messages have been sent.
     */
    void  close_n_release() {
        if (channelRef.get() != null) 
            channelRef.get().close().awaitUninterruptibly();
        //we are now ready to release resources
        ready_to_release_resource.set(true);
    }

    public TaskMessage recv(int flags) {
        throw new RuntimeException("Client connection should not receive any messages");
    }

    void setChannel(Channel channel) {
        channelRef.set(channel); 
    }

}




