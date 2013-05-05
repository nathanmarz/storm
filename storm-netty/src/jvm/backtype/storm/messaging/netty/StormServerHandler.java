package backtype.storm.messaging.netty;

import java.util.concurrent.atomic.AtomicInteger;

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
    private AtomicInteger failure_count; 
    
    StormServerHandler(Server server) {
        this.server = server;
        failure_count = new AtomicInteger(0);
    }
    
    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        server.addChannel(e.getChannel());
    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        TaskMessage message = (TaskMessage)e.getMessage();  
        if (message == null) return;

        //end of batch?
        if (message.task() == Util.EOB) {
            Channel channel = ctx.getChannel();
            LOG.debug("Sendback response ...");
            if (failure_count.get()==0)
                channel.write(Util.OK_RESPONSE);
            else channel.write(Util.FAILURE_RESPONSE);
            return;
        }
        
        //enqueue the received message for processing
        try {
            server.enqueue(message);
        } catch (InterruptedException e1) {
            LOG.info("failed to enqueue a request message", e);
            failure_count.incrementAndGet();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        server.closeChannel(e.getChannel());
    }
}
