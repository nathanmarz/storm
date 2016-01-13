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

import org.apache.storm.utils.Utils;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class StormServerHandler extends SimpleChannelUpstreamHandler  {
    private static final Logger LOG = LoggerFactory.getLogger(StormServerHandler.class);
    IServer server;
    private AtomicInteger failure_count; 
    private Channel channel;
    
    public StormServerHandler(IServer server) {
        this.server = server;
        failure_count = new AtomicInteger(0);
    }
    
    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        server.channelConnected(e.getChannel());
        if(channel != null) {
            LOG.debug("Replacing channel with new channel: {} -> ",
                      channel, e.getChannel());
        }
        channel = e.getChannel();
        server.channelConnected(channel);
    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      Object msgs = e.getMessage();
      if (msgs == null) {
        return;
      }
      
      try {
        server.received(msgs, e.getRemoteAddress().toString(), channel);
      } catch (InterruptedException e1) {
        LOG.info("failed to enqueue a request message", e);
        failure_count.incrementAndGet();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        LOG.error("server errors in handling the request", e.getCause());
        Utils.handleUncaughtException(e.getCause());
        server.closeChannel(e.getChannel());
    }
}
