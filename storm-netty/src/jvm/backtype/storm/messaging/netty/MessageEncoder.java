package backtype.storm.messaging.netty;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

public class MessageEncoder extends OneToOneEncoder {    
    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object obj) throws Exception {
        if (obj instanceof ControlMessage) {
            return ((ControlMessage)obj).buffer();
        }

        if (obj instanceof MessageBatch) {
            return ((MessageBatch)obj).buffer();
        } 
        
        throw new RuntimeException("Unsupported encoding of object of class "+obj.getClass().getName());
    }


}
