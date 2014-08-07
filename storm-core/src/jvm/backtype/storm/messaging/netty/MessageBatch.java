/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.messaging.netty;

import backtype.storm.messaging.TaskMessage;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;

import java.util.ArrayList;

class MessageBatch {
    private int buffer_size;
    private ArrayList<TaskMessage> msgs;
    private int encoded_length;

    MessageBatch(int buffer_size) {
        this.buffer_size = buffer_size;
        msgs = new ArrayList<TaskMessage>();
        encoded_length = ControlMessage.EOB_MESSAGE.encodeLength();
    }

    void add(TaskMessage obj) {
        if (obj == null)
            throw new RuntimeException("null object forbidded in message batch");

        TaskMessage msg = (TaskMessage)obj;
        msgs.add(msg);
        encoded_length += msgEncodeLength(msg);
    }


    TaskMessage get(int index) {
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
        
        for (TaskMessage msg : msgs)
            writeTaskMessage(bout, msg);

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