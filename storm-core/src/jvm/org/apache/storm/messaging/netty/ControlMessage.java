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
package org.apache.storm.messaging.netty;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;

public enum ControlMessage implements INettySerializable {
    CLOSE_MESSAGE((short)-100),
    EOB_MESSAGE((short)-201),
    OK_RESPONSE((short)-200),
    FAILURE_RESPONSE((short)-400),
    SASL_TOKEN_MESSAGE_REQUEST((short)-202),
    SASL_COMPLETE_REQUEST((short)-203);

    private short code;

    //private constructor
    private ControlMessage(short code) {
        this.code = code;
    }

    /**
     * @param encoded status code
     * @return a control message per an encoded status code
     */
    public static ControlMessage mkMessage(short encoded) {
        for(ControlMessage cm: ControlMessage.values()) {
          if(encoded == cm.code) return cm;
        }
        return null;
    }

    public int encodeLength() {
        return 2; //short
    }
    
    /**
     * encode the current Control Message into a channel buffer
     * @throws IOException
     */
    public ChannelBuffer buffer() throws IOException {
        ChannelBufferOutputStream bout = new ChannelBufferOutputStream(ChannelBuffers.directBuffer(encodeLength()));      
        write(bout);
        bout.close();
        return bout.buffer();
    }

    public static ControlMessage read(byte[] serial) {
        ChannelBuffer cm_buffer = ChannelBuffers.copiedBuffer(serial);
        return mkMessage(cm_buffer.getShort(0));
    }
    
    public void write(ChannelBufferOutputStream bout) throws IOException {
        bout.writeShort(code);        
    } 
}
