package backtype.storm.messaging.netty;

import java.util.ArrayList;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import backtype.storm.Config;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.Utils;

public class MessageEncoder extends OneToOneEncoder {    
    @SuppressWarnings("unchecked")
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
