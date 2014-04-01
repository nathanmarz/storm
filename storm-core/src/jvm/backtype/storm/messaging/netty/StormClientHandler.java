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

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicBoolean;

public class StormClientHandler extends SimpleChannelUpstreamHandler  {
    private static final Logger LOG = LoggerFactory.getLogger(StormClientHandler.class);
    private Client client;
    long start_time;
    
    StormClientHandler(Client client) {
        this.client = client;
        start_time = System.currentTimeMillis();
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent event) {
        //register the newly established channel
        Channel channel = event.getChannel();
        client.setChannel(channel);
        LOG.info("connection established from "+channel.getLocalAddress()+" to "+channel.getRemoteAddress());
        
        //send next batch of requests if any
        try {
            client.tryDeliverMessages(false);
        } catch (Exception ex) {
            LOG.info("exception when sending messages:", ex.getMessage());
            client.reconnect();
        }
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) {
        LOG.debug("send/recv time (ms): {}", (System.currentTimeMillis() - start_time));
        
        //examine the response message from server
        ControlMessage msg = (ControlMessage)event.getMessage();
        if (msg==ControlMessage.FAILURE_RESPONSE)
            LOG.info("failure response:{}", msg);

        //send next batch of requests if any
        try {
            client.tryDeliverMessages(false);
        } catch (Exception ex) {
            LOG.info("exception when sending messages:", ex.getMessage());
            client.reconnect();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event) {
        Throwable cause = event.getCause();
        if (!(cause instanceof ConnectException)) {
            LOG.info("Connection to "+client.remote_addr+" failed:", cause);
        }
        client.reconnect();
    }
}
