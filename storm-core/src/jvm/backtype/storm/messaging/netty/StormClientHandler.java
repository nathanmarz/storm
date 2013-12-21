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

import org.jboss.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicBoolean;

public class StormClientHandler extends SimpleChannelUpstreamHandler  {
    private static final Logger LOG = LoggerFactory.getLogger(StormClientHandler.class);
    private Client client;
    private AtomicBoolean being_closed;
    long start_time; 
    
    StormClientHandler(Client client) {
        this.client = client;
        being_closed = new AtomicBoolean(false);
        start_time = System.currentTimeMillis();
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent event) {
        //register the newly established channel
        Channel channel = event.getChannel();
        client.setChannel(channel);
        LOG.debug("connection established to a remote host");
        
        //send next request
        try {
            sendRequests(channel, client.takeMessages());
        } catch (InterruptedException e) {
            channel.close();
        }
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) {
        LOG.debug("send/recv time (ms): {}", (System.currentTimeMillis() - start_time));
        
        //examine the response message from server
        ControlMessage msg = (ControlMessage)event.getMessage();
        if (msg==ControlMessage.FAILURE_RESPONSE)
            LOG.info("failure response:{}", msg);

        //send next request
        Channel channel = event.getChannel();
        try {
            sendRequests(channel, client.takeMessages());
        } catch (InterruptedException e) {
            channel.close();
        }
    }

    /**
     * Retrieve a request from message queue, and send to server
     * @param channel
     */
    private void sendRequests(Channel channel, final MessageBatch requests) {
        if (requests==null || requests.size()==0 || being_closed.get()) return;

        //if task==CLOSE_MESSAGE for our last request, the channel is to be closed
        Object last_msg = requests.get(requests.size()-1);
        if (last_msg==ControlMessage.CLOSE_MESSAGE) {
            being_closed.set(true);
            requests.remove(last_msg);
        }

        //we may don't need do anything if no requests found
        if (requests.isEmpty()) {
            if (being_closed.get())
                client.close_n_release();
            return;
        }

        //write request into socket channel
        ChannelFuture future = channel.write(requests);
        future.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future)
                    throws Exception {
                if (!future.isSuccess()) {
                    LOG.info("failed to send requests:", future.getCause());
                    future.getChannel().close();
                } else {
                    LOG.debug("{} request(s) sent", requests.size());
                }
                if (being_closed.get())
                    client.close_n_release();
            }
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event) {
        Throwable cause = event.getCause();
        if (!(cause instanceof ConnectException)) {
            LOG.info("Connection failed:", cause);
        } 
        if (!being_closed.get()) {
            client.setChannel(null);
            client.reconnect();
        }
    }
}
