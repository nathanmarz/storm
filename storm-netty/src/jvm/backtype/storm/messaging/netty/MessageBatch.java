package backtype.storm.messaging.netty;

import java.util.ArrayList;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;

import backtype.storm.messaging.TaskMessage;

class MessageBatch {
    private int buffer_size;
    private ArrayList<Object> msgs;
    private int encoded_length;

    MessageBatch(int buffer_size) {
        this.buffer_size = buffer_size;
        msgs = new ArrayList<Object>();
        encoded_length = ControlMessage.EOB_MESSAGE.encodeLength();
    }

    void add(Object obj) {
        if (obj == null)
            throw new RuntimeException("null object forbidded in message batch");

        if (obj instanceof TaskMessage) {
            TaskMessage msg = (TaskMessage)obj;
            msgs.add(msg);
            encoded_length += msgEncodeLength(msg);
            return;
        }

        if (obj instanceof ControlMessage) {
            ControlMessage msg = (ControlMessage)obj;
            msgs.add(msg);
            encoded_length += msg.encodeLength();
            return;
        }

        throw new RuntimeException("Unsuppoted object type "+obj.getClass().getName());
    }

    void remove(Object obj) {
        if (obj == null) return;

        if (obj instanceof TaskMessage) {
            TaskMessage msg = (TaskMessage)obj;
            msgs.remove(msg);
            encoded_length -= msgEncodeLength(msg);
            return;
        }

        if (obj instanceof ControlMessage) {
            ControlMessage msg = (ControlMessage)obj;
            msgs.remove(msg);
            encoded_length -= msg.encodeLength();
            return;
        }
    }

    Object get(int index) {
        return msgs.get(index);
    }

    /**
     * try to add a TaskMessage to a batch
     * @param taskMsg
     * @return false if the msg could not be added due to buffer size limit; true otherwise
     */
    boolean tryAdd(TaskMessage taskMsg) {
        if ((encoded_length + msgEncodeLength(taskMsg)) > buffer_size) 
            return false;
        add(taskMsg);
        return true;
    }

    private int msgEncodeLength(TaskMessage taskMsg) {
        if (taskMsg == null) return 0;

        int size = 6; //INT + SHORT
        if (taskMsg.message() != null) 
            size += taskMsg.message().length;
        return size;
    }

    /**
     * Has this batch used up allowed buffer size
     * @return
     */
    boolean isFull() {
        return encoded_length >= buffer_size;
    }

    /**
     * true if this batch doesn't have any messages 
     * @return
     */
    boolean isEmpty() {
        return msgs.isEmpty();
    }

    /**
     * # of msgs in this batch
     * @return
     */
    int size() {
        return msgs.size();
    }

    /**
     * create a buffer containing the encoding of this batch
     */
    ChannelBuffer buffer() throws Exception {
        ChannelBufferOutputStream bout = new ChannelBufferOutputStream(ChannelBuffers.directBuffer(encoded_length));
        
        for (Object msg : msgs) 
            if (msg instanceof TaskMessage)
                writeTaskMessage(bout, (TaskMessage)msg);
            else
                ((ControlMessage)msg).write(bout);
        
        //add a END_OF_BATCH indicator
        ControlMessage.EOB_MESSAGE.write(bout);

        bout.close();

        return bout.buffer();
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

        int task_id = message.task();
        if (task_id > Short.MAX_VALUE)
            throw new RuntimeException("Task ID should not exceed "+Short.MAX_VALUE);
        
        bout.writeShort((short)task_id);
        bout.writeInt(payload_len);
        if (payload_len >0)
            bout.write(message.message());
    }
}