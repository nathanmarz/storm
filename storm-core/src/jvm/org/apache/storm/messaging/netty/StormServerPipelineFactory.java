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

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;

import org.apache.storm.Config;

class StormServerPipelineFactory implements ChannelPipelineFactory {
    private Server server;

    StormServerPipelineFactory(Server server) {
        this.server = server;
    }

    public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = Channels.pipeline();

        // Decoder
        pipeline.addLast("decoder", new MessageDecoder());
        // Encoder
        pipeline.addLast("encoder", new MessageEncoder());

        boolean isNettyAuth = (Boolean) this.server.storm_conf
                .get(Config.STORM_MESSAGING_NETTY_AUTHENTICATION);
        if (isNettyAuth) {
            // Authenticate: Removed after authentication completes
            pipeline.addLast("saslServerHandler", new SaslStormServerHandler(
                    server));
            // Authorize
            pipeline.addLast("authorizeServerHandler",
                    new SaslStormServerAuthorizeHandler());
        }
        // business logic.
        pipeline.addLast("handler", new StormServerHandler(server));

        return pipeline;
    }
}
