package backtype.storm.messaging.netty;

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
    
    StormClientHandler(Client client) {
        this.client = client;
        being_closed = new AtomicBoolean(false);
    }
    
    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        //register the newly established channel
        Channel channel = e.getChannel();
        client.setChannel(channel);
        
        //send next request
        sendRequest(ctx.getChannel());
    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        
        //examine the response message from server
        TaskMessage msg = (TaskMessage)e.getMessage();
        if (msg.task()!=Util.OK)
            LOG.info("failure response:"+msg);
        
        //send next request
        if (!being_closed.get()) 
            sendRequest(ctx.getChannel());
    }

    /**
     * Retrieve a request from message queue, and send to server
     * @param channel
     */
    private void sendRequest(Channel channel) {
        try {
            //retrieve request from queue
            final ArrayList<TaskMessage> requests = client.takeMessages();
            //if task==SHUTDOWN for any request, the channel is to be closed
            for (TaskMessage message: requests) {
                if (message.task()==Util.CLOSE) {
                    being_closed.set(true);
                    requests.remove(message);
                    break;
                }
            }
                        
            //we may don't need do anything if no requests found
            if (requests.isEmpty()) {
                if (being_closed.get())
                    client.close_n_release();
                return;
            }
            
            //write request into socket channel
            ChannelFuture future = channel.write(requests);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future)
                        throws Exception {
                    if (!future.isSuccess()) {
                        LOG.info("failed to send requests:", future.getCause());
                        future.getChannel().close();
                    } else {
                        LOG.debug(requests.size() + " request(s) sent");
                    }
                    if (being_closed.get())
                        client.close_n_release();
                }
            });
            
        } catch (InterruptedException e1) {
            channel.close();
        }
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        if (!being_closed.get())
            client.reconnect();
    }
}
