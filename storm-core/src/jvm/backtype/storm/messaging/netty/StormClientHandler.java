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
import backtype.storm.serialization.KryoValuesDeserializer;

import java.net.ConnectException;
import java.util.Map;
import java.util.List;
import java.io.IOException;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import org.jboss.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormClientHandler extends SimpleChannelUpstreamHandler  {
    private static final Logger LOG = LoggerFactory.getLogger(StormClientHandler.class);
    private Client client;
    private KryoValuesDeserializer _des;

    StormClientHandler(Client client, Map conf) {
        this.client = client;
        _des = new KryoValuesDeserializer(conf);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) {
        //examine the response message from server
        Object message = event.getMessage();
        if (message instanceof ControlMessage) {
          ControlMessage msg = (ControlMessage)message;
          if (msg==ControlMessage.FAILURE_RESPONSE)
              LOG.info("failure response:{}", msg);

        } else {
          throw new RuntimeException("Don't know how to handle a message of type "+message+" ("+client.getDstAddress()+")");
        }
    }
        
    @Override
    public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        client.notifyInterestChanged(e.getChannel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event) {
        Throwable cause = event.getCause();
        if (!(cause instanceof ConnectException)) {
            LOG.info("Connection to "+client.getDstAddress()+" failed:", cause);
        }
    }
}
