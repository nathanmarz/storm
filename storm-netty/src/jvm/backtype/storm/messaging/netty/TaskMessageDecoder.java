package backtype.storm.messaging.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import backtype.storm.messaging.TaskMessage;

class TaskMessageDecoder extends FrameDecoder {    
    /*
     * Each TaskMessage is encoded as:
     *  task ... short(2)
     *  len ... int(4)
     *  payload ... byte[]     *  
     */
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {
        // Make sure if both task and len were received.
        if (buf.readableBytes() < 6) {
            //need more data
            return null;
        }

        // Mark the current buffer position before reading task/len field
        // because the whole frame might not be in the buffer yet.
        // We will reset the buffer position to the marked position if
        // there's not enough bytes in the buffer.
        buf.markReaderIndex();

        //read task field
        short task = buf.readShort();
        
        // Read the length field.
        int length = buf.readInt();
        if (length==0) {
            return new TaskMessage(task, null);
        }
        
        // Make sure if there's enough bytes in the buffer.
        if (buf.readableBytes() < length) {
            // The whole bytes were not received yet - return null.
            // This method will be invoked again when more packets are
            // received and appended to the buffer.

            // Reset to the marked position to read the length field again
            // next time.
            buf.resetReaderIndex();

            return null;
        }

        // There's enough bytes in the buffer. Read it.
        ChannelBuffer payload = buf.readBytes(length);

        // Successfully decoded a frame.
        // Return a TaskMessage object
        return new TaskMessage(task,payload.array());
    }
}