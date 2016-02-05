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

import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channel;
import org.apache.storm.generated.HBMessage;
import org.apache.storm.generated.HBMessageData;
import org.apache.storm.generated.HBServerMessageType;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.ChannelBuffer;
import org.apache.storm.utils.Utils;
import org.apache.storm.messaging.netty.ControlMessage;
import org.apache.storm.messaging.netty.SaslMessageToken;
import org.apache.storm.messaging.netty.INettySerializable;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.thrift.TBase;

public class ThriftEncoder extends OneToOneEncoder {

    private static final Logger LOG = LoggerFactory
        .getLogger(ThriftEncoder.class);

    private HBMessage encodeNettySerializable(INettySerializable netty_message,
                                              HBServerMessageType mType) {
        
        HBMessageData message_data = new HBMessageData();
        HBMessage m = new HBMessage();
        try {
            ChannelBuffer cbuffer = netty_message.buffer();
            if(cbuffer.hasArray()) {
                message_data.set_message_blob(cbuffer.array());
            }
            else {
                byte buff[] = new byte[netty_message.encodeLength()];
                cbuffer.readBytes(buff, 0, netty_message.encodeLength());
                message_data.set_message_blob(buff);
            }
            m.set_type(mType);
            m.set_data(message_data);
            return m;
        }
        catch( IOException e) {
            LOG.error("Failed to encode NettySerializable: ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) {
        if(msg == null) {
            return null;
        }

        LOG.debug("Trying to encode: " + msg.getClass().toString() + " : " + msg.toString());

        HBMessage m;
        if(msg instanceof INettySerializable) {
            INettySerializable nettyMsg = (INettySerializable)msg;

            HBServerMessageType type;
            if(msg instanceof ControlMessage) {
                type = HBServerMessageType.CONTROL_MESSAGE;
            }
            else if(msg instanceof SaslMessageToken) {
                type = HBServerMessageType.SASL_MESSAGE_TOKEN;
            }
            else {
                LOG.error("Didn't recognise INettySerializable: " + nettyMsg.toString());
                throw new RuntimeException("Unrecognized INettySerializable.");
            }
            m = encodeNettySerializable(nettyMsg, type);
        }
        else {
            m = (HBMessage)msg;
        }

        try {
            byte serialized[] = Utils.thriftSerialize(m);
            ChannelBuffer ret = ChannelBuffers.directBuffer(serialized.length + 4);

            ret.writeInt(serialized.length);
            ret.writeBytes(serialized);

            return ret;
        }
        catch (RuntimeException e) {
            LOG.error("Failed to serialize.", e);
            throw e;
        }
    }
}
