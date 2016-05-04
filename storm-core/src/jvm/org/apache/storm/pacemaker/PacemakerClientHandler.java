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
package org.apache.storm.pacemaker;

import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.Channel;
import org.apache.storm.generated.HBMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.messaging.netty.ControlMessage;

public class PacemakerClientHandler extends SimpleChannelUpstreamHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PacemakerClientHandler.class);

    private PacemakerClient client;



    public PacemakerClientHandler(PacemakerClient client) {
        this.client = client;
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx,
                                 ChannelStateEvent event) {
        // register the newly established channel
        Channel channel = ctx.getChannel();
        client.channelConnected(channel);

        LOG.info("Connection established from {} to {}",
                 channel.getLocalAddress(), channel.getRemoteAddress());
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) {
        LOG.debug("Got Message: {}", event.getMessage().toString());
        Object evm = event.getMessage();

        if(evm instanceof ControlMessage) {
            LOG.debug("Got control message: {}", evm.toString());
            return;
        }
        else if(evm instanceof HBMessage) {
            client.gotMessage((HBMessage)evm);
        }
        else {
            LOG.warn("Got unexpected message: {} from server.", evm);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event) {
        LOG.error("Connection to pacemaker failed", event.getCause());
        client.reconnect();
    }
}
