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
    int estimated_buffer_size;
    
    @SuppressWarnings("rawtypes")
    MessageEncoder(Map conf) {
        estimated_buffer_size = Utils.getInt(conf.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
    }
    
    @SuppressWarnings("unchecked")
    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object obj) throws Exception {
        
        if (obj instanceof ControlMessage) {
            ControlMessage message = (ControlMessage)obj;
            ChannelBufferOutputStream bout =
                    new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(
                            estimated_buffer_size, ctx.getChannel().getConfig().getBufferFactory()));
            writeControlMessage(bout, message);
            bout.close();

            return bout.buffer();
        }

        if (obj instanceof TaskMessage) {
            TaskMessage message = (TaskMessage)obj;
            ChannelBufferOutputStream bout =
                    new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(
                            estimated_buffer_size, ctx.getChannel().getConfig().getBufferFactory()));
            writeTaskMessage(bout, message);
            bout.close();

            return bout.buffer();
        }

        if (obj instanceof ArrayList<?>) {
            ChannelBufferOutputStream bout =
                    new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(
                            estimated_buffer_size, ctx.getChannel().getConfig().getBufferFactory()));
            ArrayList<TaskMessage> messages = (ArrayList<TaskMessage>) obj;
            for (TaskMessage message : messages) 
                writeTaskMessage(bout, message);
            writeControlMessage(bout, ControlMessage.EOB_MESSAGE);
            bout.close();

            return bout.buffer();
        }

        return null;
    }

    /**
     * write a TaskMessage into a stream
     *
     * Each TaskMessage is encoded as:
     *  task ... short(2)
     *  len ... int(4)
     *  payload ... byte[]     *  
     */
    private void writeTaskMessage(ChannelBufferOutputStream bout, TaskMessage message) throws Exception {
        int payload_len = 0;
        if (message.message() != null)
            payload_len =  message.message().length;

        bout.writeShort((short)message.task());
        bout.writeInt(payload_len);
        if (payload_len >0)
            bout.write(message.message());
    }

    /**
     * write a ControlMessage into a stream
     *
     * Each TaskMessage is encoded as:
     *  code ... short(2)
     */
    private void writeControlMessage(ChannelBufferOutputStream bout, ControlMessage message) throws Exception {
        bout.writeShort(message.code());
    }
}
