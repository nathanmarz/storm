package backtype.storm.messaging.netty;

import backtype.storm.messaging.TaskMessage;
import org.jboss.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

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
        Object msg = e.getMessage();  
        if (msg == null) return;

        //end of batch?
        if (msg==ControlMessage.EOB_MESSAGE) {
            Channel channel = ctx.getChannel();
            LOG.debug("Send back response ...");
            if (failure_count.get()==0)
                channel.write(ControlMessage.OK_RESPONSE);
            else channel.write(ControlMessage.FAILURE_RESPONSE);
            return;
        }
        
        //enqueue the received message for processing
        try {
            server.enqueue((TaskMessage)msg);
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
