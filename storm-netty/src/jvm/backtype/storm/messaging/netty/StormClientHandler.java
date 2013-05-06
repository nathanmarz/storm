package backtype.storm.messaging.netty;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.TaskMessage;

class StormClientHandler extends SimpleChannelUpstreamHandler  {
    private static final Logger LOG = LoggerFactory.getLogger(StormClientHandler.class);
    private Client client;
    private AtomicBoolean being_closed;
    long start_time; 
    
    StormClientHandler(Client client) {
        this.client = client;
        being_closed = new AtomicBoolean(false);
        start_time = System.currentTimeMillis();
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent event) {
        //register the newly established channel
        Channel channel = event.getChannel();
        client.setChannel(channel);
        LOG.debug("connection established to a remote host");
        
        //send next request
        try {
            sendRequests(channel, client.takeMessages());
        } catch (InterruptedException e) {
            channel.close();
        }
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) {
        LOG.debug("send/recv time (ms):"+(System.currentTimeMillis() - start_time));
        
        //examine the response message from server
        TaskMessage msg = (TaskMessage)event.getMessage();
        if (msg.task()!=Util.OK)
            LOG.info("failure response:"+msg);

        //send next request
        Channel channel = event.getChannel();
        try {
            sendRequests(channel, client.takeMessages());
        } catch (InterruptedException e) {
            channel.close();
        }
    }

    /**
     * Retrieve a request from message queue, and send to server
     * @param channel
     */
    private void sendRequests(Channel channel, final ArrayList<TaskMessage> requests) {
        if (being_closed.get()) return;

        //if task==CLOSE_MESSAGE for our last request, the channel is to be closed
        TaskMessage last_msg = requests.get(requests.size()-1);
        if (last_msg==Util.CLOSE_MESSAGE) {
            being_closed.set(true);
            requests.remove(last_msg);
        }

        //we may don't need do anything if no requests found
        if (requests.isEmpty()) {
            if (being_closed.get())
                client.close_n_release();
            return;
        }

        //add an EOB_MESSAGE to the end of our batch
        requests.add(Util.EOB_MESSAGE);
        //write request into socket channel
        ChannelFuture future = channel.write(requests);
        future.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future)
                    throws Exception {
                if (!future.isSuccess()) {
                    LOG.info("failed to send requests:", future.getCause());
                    future.getChannel().close();
                } else {
                    LOG.debug((requests.size()-1) + " request(s) sent");
                }
                if (being_closed.get())
                    client.close_n_release();
            }
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event) {
        Throwable cause = event.getCause();
        if (!(cause instanceof ConnectException)) {
            LOG.info("Connection failed:"+cause.getMessage(), cause);
        } 
        if (!being_closed.get()) {
            client.setChannel(null);
            client.reconnect();
        }
    }
}
