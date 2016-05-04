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

import org.apache.storm.messaging.netty.KerberosSaslClientHandler;
import org.apache.storm.messaging.netty.SaslStormClientHandler;
import org.apache.storm.security.auth.AuthUtils;
import java.io.IOException;
import java.util.Map;
import org.apache.storm.pacemaker.PacemakerClient;
import org.apache.storm.pacemaker.PacemakerClientHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftNettyClientCodec {

    public static final String SASL_HANDLER = "sasl-handler";
    public static final String KERBEROS_HANDLER = "kerberos-handler";
    
    public enum AuthMethod {
        DIGEST,
        KERBEROS,
        NONE
    };

    private static final Logger LOG = LoggerFactory
        .getLogger(ThriftNettyClientCodec.class);

    private PacemakerClient client;
    private AuthMethod authMethod;
    private Map storm_conf;

    public ThriftNettyClientCodec(PacemakerClient pacemaker_client, Map storm_conf, AuthMethod authMethod) {
        client = pacemaker_client;
        this.authMethod = authMethod;
        this.storm_conf = storm_conf;
    }

    public ChannelPipelineFactory pipelineFactory() {
        return new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("encoder", new ThriftEncoder());
                pipeline.addLast("decoder", new ThriftDecoder());

                if (authMethod == AuthMethod.KERBEROS) {
                    try {
                        LOG.debug("Adding KerberosSaslClientHandler to pacemaker client pipeline.");
                        pipeline.addLast(KERBEROS_HANDLER,
                                         new KerberosSaslClientHandler(client,
                                                                       storm_conf,
                                                                       AuthUtils.LOGIN_CONTEXT_PACEMAKER_CLIENT));
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                else if(authMethod == AuthMethod.DIGEST) {
                    try {
                        LOG.debug("Adding SaslStormClientHandler to pacemaker client pipeline.");
                        pipeline.addLast(SASL_HANDLER, new SaslStormClientHandler(client));
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                else {
                    client.channelReady();
                }

                pipeline.addLast("PacemakerClientHandler", new PacemakerClientHandler(client));
                return pipeline;
            }
        };
    }
}
