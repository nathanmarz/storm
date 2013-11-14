package backtype.storm.messaging.netty;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.Utils;

class Server implements IConnection {
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);
    @SuppressWarnings("rawtypes")
    Map storm_conf;
    int port;
    private LinkedBlockingQueue<TaskMessage> message_queue;
    volatile ChannelGroup allChannels = new DefaultChannelGroup("storm-server");
    final ChannelFactory factory;
    final ServerBootstrap bootstrap;

    @SuppressWarnings("rawtypes")
    Server(Map storm_conf, int port) {
        this.storm_conf = storm_conf;
        this.port = port;
        message_queue = new LinkedBlockingQueue<TaskMessage>();

        // Configure the server.
        int buffer_size = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        int maxWorkers = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS));

        if (maxWorkers > 0) {
            factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), maxWorkers);
        } else {
            factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
        }
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

    /**
     * enqueue a received message 
     * @param message
     * @throws InterruptedException
     */
    protected void enqueue(TaskMessage message) throws InterruptedException {
        message_queue.put(message);
        LOG.debug("message received with task: {}, payload size: {}", message.task(), message.message().length);
    }
    
    /**
     * fetch a message from message queue synchronously (flags != 1) or asynchronously (flags==1)
     */
    public TaskMessage recv(int flags)  {
        if ((flags & 0x01) == 0x01) { 
            //non-blocking
            return message_queue.poll();
        } else {
            try {
                TaskMessage request = message_queue.take();
                LOG.debug("request to be processed: {}", request);
                return request;
            } catch (InterruptedException e) {
                LOG.info("exception within msg receiving", e);
                return null;
            }
        }
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
}
