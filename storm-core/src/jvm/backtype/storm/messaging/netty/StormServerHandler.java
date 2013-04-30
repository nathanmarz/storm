package backtype.storm.messaging.netty;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.TaskMessage;

class StormServerHandler extends SimpleChannelUpstreamHandler  {
    private static final Logger LOG = LoggerFactory.getLogger(StormServerHandler.class);
    Server server;

    StormServerHandler(Server server) {
        this.server = server;
    }
    
    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        server.addChannel(e.getChannel());
    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        TaskMessage message = (TaskMessage)e.getMessage();  
        if (message == null) return;

        //receive next request
        boolean success = true;
        try {
            server.enqueue(message);
        } catch (InterruptedException e1) {
            LOG.error("failed to enqueue a request message", e);
            success = false;
        }

        //send ack
        Channel channel = ctx.getChannel();
        if (success) channel.write(Util.OK_RESPONSE);
        else channel.write(Util.FAILURE_RESPONSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        server.closeChannel(e.getChannel());
    }
}
