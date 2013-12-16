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
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class MessageDecoder extends FrameDecoder {    
    /*
     * Each ControlMessage is encoded as:
     *  code (<0) ... short(2)
     * Each TaskMessage is encoded as:
     *  task (>=0) ... short(2)
     *  len ... int(4)
     *  payload ... byte[]     *  
     */
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {
        // Make sure that we have received at least a short 
        if (buf.readableBytes() < 2) {
            //need more data
            return null;
        }

        // Mark the current buffer position before reading task/len field
        // because the whole frame might not be in the buffer yet.
        // We will reset the buffer position to the marked position if
        // there's not enough bytes in the buffer.
        buf.markReaderIndex();

        //read the short field
        short code = buf.readShort();
        
        //case 1: Control message
        ControlMessage ctrl_msg = ControlMessage.mkMessage(code);
        if (ctrl_msg != null) return ctrl_msg;
        
        //case 2: task Message
        short task = code;
        
        // Make sure that we have received at least an integer (length) 
        if (buf.readableBytes() < 4) {
            //need more data
            buf.resetReaderIndex();
            return null;
        }

        // Read the length field.
        int length = buf.readInt();
        if (length<=0) {
            return new TaskMessage(task, null);
        }
        
        // Make sure if there's enough bytes in the buffer.
        if (buf.readableBytes() < length) {
            // The whole bytes were not received yet - return null.
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