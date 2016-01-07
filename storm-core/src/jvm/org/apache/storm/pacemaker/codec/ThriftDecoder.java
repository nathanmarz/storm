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
package org.apache.storm.pacemaker.codec;

import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channel;
import backtype.storm.generated.HBMessage;
import backtype.storm.generated.HBServerMessageType;
import org.jboss.netty.buffer.ChannelBuffer;
import backtype.storm.utils.Utils;
import backtype.storm.messaging.netty.ControlMessage;
import backtype.storm.messaging.netty.SaslMessageToken;

public class ThriftDecoder extends FrameDecoder {

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {

        long available = buf.readableBytes();
        if(available < 2) {
            return null;
        }

        buf.markReaderIndex();

        int thriftLen = buf.readInt();
        available -= 4;

        if(available < thriftLen) {
            // We haven't received the entire object yet, return and wait for more bytes.
            buf.resetReaderIndex();
            return null;
        }

        buf.discardReadBytes();

        HBMessage m;
        if(buf.hasArray()) {
            m = Utils.thriftDeserialize(HBMessage.class, buf.array(), 0, thriftLen);
            buf.readerIndex(buf.readerIndex() + thriftLen);
        }
        else {
            byte serialized[] = new byte[thriftLen];
            buf.readBytes(serialized, 0, thriftLen);
            m = Utils.thriftDeserialize(HBMessage.class, serialized);
        }

        if(m.get_type() == HBServerMessageType.CONTROL_MESSAGE) {
            ControlMessage cm = ControlMessage.read(m.get_data().get_message_blob());
            return cm;
        }
        else if(m.get_type() == HBServerMessageType.SASL_MESSAGE_TOKEN) {
            SaslMessageToken sm = SaslMessageToken.read(m.get_data().get_message_blob());
            return sm;
        }
        else {
            return m;
        }
    }
}
