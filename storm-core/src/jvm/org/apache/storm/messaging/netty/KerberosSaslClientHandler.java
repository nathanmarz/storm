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
import java.util.Map;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosSaslClientHandler extends SimpleChannelUpstreamHandler {

    private static final Logger LOG = LoggerFactory
        .getLogger(KerberosSaslClientHandler.class);
    private ISaslClient client;
    long start_time;
    /** Used for client or server's token to send or receive from each other. */
    private Map storm_conf;
    private String jaas_section;

    public KerberosSaslClientHandler(ISaslClient client, Map storm_conf, String jaas_section) throws IOException {
        this.client = client;
        this.storm_conf = storm_conf;
        this.jaas_section = jaas_section;
        start_time = System.currentTimeMillis();
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx,
                                 ChannelStateEvent event) {
        // register the newly established channel
        Channel channel = ctx.getChannel();
        client.channelConnected(channel);

        LOG.info("Connection established from {} to {}",
                 channel.getLocalAddress(), channel.getRemoteAddress());

        try {
            KerberosSaslNettyClient saslNettyClient = KerberosSaslNettyClientState.getKerberosSaslNettyClient
                .get(channel);

            if (saslNettyClient == null) {
                LOG.debug("Creating saslNettyClient now for channel: {}",
                          channel);
                saslNettyClient = new KerberosSaslNettyClient(storm_conf, jaas_section);
                KerberosSaslNettyClientState.getKerberosSaslNettyClient.set(channel,
                                                                            saslNettyClient);
            }
            LOG.debug("Going to initiate Kerberos negotiations.");
            byte[] initialChallenge = saslNettyClient.saslResponse(new SaslMessageToken(new byte[0]));
            LOG.debug("Sending initial challenge: {}", initialChallenge);
            channel.write(new SaslMessageToken(initialChallenge));
        } catch (Exception e) {
            LOG.error("Failed to authenticate with server due to error: ",
                      e);
        }
        return;

    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event)
        throws Exception {
        LOG.debug("send/recv time (ms): {}",
                  (System.currentTimeMillis() - start_time));

        Channel channel = ctx.getChannel();

        // Generate SASL response to server using Channel-local SASL client.
        KerberosSaslNettyClient saslNettyClient = KerberosSaslNettyClientState.getKerberosSaslNettyClient
            .get(channel);
        if (saslNettyClient == null) {
            throw new Exception("saslNettyClient was unexpectedly null for channel:" + channel);
        }

        // examine the response message from server
        if (event.getMessage() instanceof ControlMessage) {
            ControlMessage msg = (ControlMessage) event.getMessage();
            if (msg == ControlMessage.SASL_COMPLETE_REQUEST) {
                LOG.debug("Server has sent us the SaslComplete message. Allowing normal work to proceed.");

                if (!saslNettyClient.isComplete()) {
                    String message = "Server returned a Sasl-complete message, but as far as we can tell, we are not authenticated yet.";
                    LOG.error(message);
                    throw new Exception(message);
                }
                ctx.getPipeline().remove(this);
                this.client.channelReady();

                // We call fireMessageReceived since the client is allowed to
                // perform this request. The client's request will now proceed
                // to the next pipeline component namely StormClientHandler.
                Channels.fireMessageReceived(ctx, msg);
            } else {
                LOG.warn("Unexpected control message: {}", msg);
            }
            return;
        }
        else if (event.getMessage() instanceof SaslMessageToken) {
            SaslMessageToken saslTokenMessage = (SaslMessageToken) event
                .getMessage();
            LOG.debug("Responding to server's token of length: {}",
                      saslTokenMessage.getSaslToken().length);

            // Generate SASL response (but we only actually send the response if
            // it's non-null.
            byte[] responseToServer = saslNettyClient
                .saslResponse(saslTokenMessage);
            if (responseToServer == null) {
                // If we generate a null response, then authentication has completed
                // (if not, warn), and return without sending a response back to the
                // server.
                LOG.debug("Response to server is null: authentication should now be complete.");
                if (!saslNettyClient.isComplete()) {
                    LOG.warn("Generated a null response, but authentication is not complete.");
                    throw new Exception("Our reponse to the server is null, but as far as we can tell, we are not authenticated yet.");
                }
                this.client.channelReady();
                return;
            } else {
                LOG.debug("Response to server token has length: {}",
                          responseToServer.length);
            }
            // Construct a message containing the SASL response and send it to the
            // server.
            SaslMessageToken saslResponse = new SaslMessageToken(responseToServer);
            channel.write(saslResponse);
        } else {
            LOG.error("Unexpected message from server: {}", event.getMessage());
        }
    }
}
