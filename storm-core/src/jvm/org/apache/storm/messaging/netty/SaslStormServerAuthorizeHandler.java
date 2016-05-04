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

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authorize or deny client requests based on existence and completeness of
 * client's SASL authentication.
 */
public class SaslStormServerAuthorizeHandler extends SimpleChannelUpstreamHandler {

	private static final Logger LOG = LoggerFactory
			.getLogger(SaslStormServerHandler.class);

	/**
	 * Constructor.
	 */
	public SaslStormServerAuthorizeHandler() {
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		Object msg = e.getMessage();
		if (msg == null)
			return;

		Channel channel = ctx.getChannel();
		LOG.debug("messageReceived: Checking whether the client is authorized to send messages to the server ");

		// Authorize: client is allowed to doRequest() if and only if the client
		// has successfully authenticated with this server.
		SaslNettyServer saslNettyServer = SaslNettyServerState.getSaslNettyServer
				.get(channel);

		if (saslNettyServer == null) {
			LOG.warn("messageReceived: This client is *NOT* authorized to perform "
					+ "this action since there's no saslNettyServer to "
					+ "authenticate the client: "
					+ "refusing to perform requested action: " + msg);
			return;
		}

		if (!saslNettyServer.isComplete()) {
			LOG.warn("messageReceived: This client is *NOT* authorized to perform "
					+ "this action because SASL authentication did not complete: "
					+ "refusing to perform requested action: " + msg);
			// Return now *WITHOUT* sending upstream here, since client
			// not authorized.
			return;
		}

		LOG.debug("messageReceived: authenticated client: "
				+ saslNettyServer.getUserName()
				+ " is authorized to do request " + "on server.");

		// We call fireMessageReceived since the client is allowed to perform
		// this request. The client's request will now proceed to the next
		// pipeline component.
		Channels.fireMessageReceived(ctx, msg);
	}
}
