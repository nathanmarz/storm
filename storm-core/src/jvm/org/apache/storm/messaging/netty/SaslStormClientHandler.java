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

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaslStormClientHandler extends SimpleChannelUpstreamHandler {

    private static final Logger LOG = LoggerFactory
            .getLogger(SaslStormClientHandler.class);

    private ISaslClient client;
    long start_time;
    /** Used for client or server's token to send or receive from each other. */
    private byte[] token;
    private String name;

    public SaslStormClientHandler(ISaslClient client) throws IOException {
        this.client = client;
        start_time = System.currentTimeMillis();
        getSASLCredentials();
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx,
            ChannelStateEvent event) {
        // register the newly established channel
        Channel channel = ctx.getChannel();
        client.channelConnected(channel);

        try {
            SaslNettyClient saslNettyClient = SaslNettyClientState.getSaslNettyClient
                    .get(channel);

            if (saslNettyClient == null) {
                LOG.debug("Creating saslNettyClient now " + "for channel: "
                        + channel);
                saslNettyClient = new SaslNettyClient(name, token);
                SaslNettyClientState.getSaslNettyClient.set(channel,
                        saslNettyClient);
            }
            LOG.debug("Sending SASL_TOKEN_MESSAGE_REQUEST");
            channel.write(ControlMessage.SASL_TOKEN_MESSAGE_REQUEST);
        } catch (Exception e) {
            LOG.error("Failed to authenticate with server " + "due to error: ",
                    e);
        }
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event)
            throws Exception {
        LOG.debug("send/recv time (ms): {}",
                (System.currentTimeMillis() - start_time));

        Channel channel = ctx.getChannel();

        // Generate SASL response to server using Channel-local SASL client.
        SaslNettyClient saslNettyClient = SaslNettyClientState.getSaslNettyClient
                .get(channel);
        if (saslNettyClient == null) {
            throw new Exception("saslNettyClient was unexpectedly "
                    + "null for channel: " + channel);
        }

        // examine the response message from server
        if (event.getMessage() instanceof ControlMessage) {
            ControlMessage msg = (ControlMessage) event.getMessage();
            if (msg == ControlMessage.SASL_COMPLETE_REQUEST) {
                LOG.debug("Server has sent us the SaslComplete "
                          + "message. Allowing normal work to proceed.");

                if (!saslNettyClient.isComplete()) {
                    LOG.error("Server returned a Sasl-complete message, "
                            + "but as far as we can tell, we are not authenticated yet.");
                    throw new Exception("Server returned a "
                            + "Sasl-complete message, but as far as "
                            + "we can tell, we are not authenticated yet.");
                }
                ctx.getPipeline().remove(this);
                this.client.channelReady();

                // We call fireMessageReceived since the client is allowed to
                // perform this request. The client's request will now proceed
                // to the next pipeline component namely StormClientHandler.
                Channels.fireMessageReceived(ctx, msg);
                return;
            }
        }
        SaslMessageToken saslTokenMessage = (SaslMessageToken) event
                .getMessage();
        LOG.debug("Responding to server's token of length: "
                  + saslTokenMessage.getSaslToken().length);

        // Generate SASL response (but we only actually send the response if
        // it's non-null.
        byte[] responseToServer = saslNettyClient
                .saslResponse(saslTokenMessage);
        if (responseToServer == null) {
            // If we generate a null response, then authentication has completed
            // (if not, warn), and return without sending a response back to the
            // server.
            LOG.debug("Response to server is null: "
                      + "authentication should now be complete.");
            if (!saslNettyClient.isComplete()) {
                LOG.warn("Generated a null response, "
                        + "but authentication is not complete.");
                throw new Exception("Server response is null, but as far as "
                        + "we can tell, we are not authenticated yet.");
            }
            this.client.channelReady();
            return;
        } else {
            LOG.debug("Response to server token has length:"
                      + responseToServer.length);
        }
        // Construct a message containing the SASL response and send it to the
        // server.
        SaslMessageToken saslResponse = new SaslMessageToken(responseToServer);
        channel.write(saslResponse);
    }

    private void getSASLCredentials() throws IOException {
        String secretKey;
        name = client.name();
        secretKey = client.secretKey();

        if (secretKey != null) {
            token = secretKey.getBytes();
        }
        LOG.debug("SASL credentials for storm topology " + name
                + " is " + secretKey);
    }
}
