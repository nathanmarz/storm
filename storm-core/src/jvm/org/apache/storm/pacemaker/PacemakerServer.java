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

import org.apache.storm.Config;
import org.apache.storm.generated.HBMessage;
import org.apache.storm.messaging.netty.ISaslServer;
import org.apache.storm.messaging.netty.NettyRenameThreadFactory;
import org.apache.storm.security.auth.AuthUtils;
import java.lang.InterruptedException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.security.auth.login.Configuration;
import org.apache.storm.pacemaker.codec.ThriftNettyServerCodec;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PacemakerServer implements ISaslServer {

    private static final long FIVE_MB_IN_BYTES = 5 * 1024 * 1024;

    private static final Logger LOG = LoggerFactory.getLogger(PacemakerServer.class);

    private final ServerBootstrap bootstrap;
    private int port;
    private IServerMessageHandler handler;
    private String secret;
    private String topo_name;
    private volatile ChannelGroup allChannels = new DefaultChannelGroup("storm-server");
    private ConcurrentSkipListSet<Channel> authenticated_channels = new ConcurrentSkipListSet<Channel>();
    private ThriftNettyServerCodec.AuthMethod authMethod;

    public PacemakerServer(IServerMessageHandler handler, Map config){
        int maxWorkers = (int)config.get(Config.PACEMAKER_MAX_THREADS);
        this.port = (int)config.get(Config.PACEMAKER_PORT);
        this.handler = handler;
        this.topo_name = "pacemaker_server";

        String auth = (String)config.get(Config.PACEMAKER_AUTH_METHOD);
        switch(auth) {

        case "DIGEST":
            Configuration login_conf = AuthUtils.GetConfiguration(config);
            authMethod = ThriftNettyServerCodec.AuthMethod.DIGEST;
            this.secret = AuthUtils.makeDigestPayload(login_conf, AuthUtils.LOGIN_CONTEXT_PACEMAKER_DIGEST);
            if(this.secret == null) {
                LOG.error("Can't start pacemaker server without digest secret.");
                throw new RuntimeException("Can't start pacemaker server without digest secret.");
            }
            break;

        case "KERBEROS":
            authMethod = ThriftNettyServerCodec.AuthMethod.KERBEROS;
            break;

        case "NONE":
            authMethod = ThriftNettyServerCodec.AuthMethod.NONE;
            break;

        default:
            LOG.error("Can't start pacemaker server without proper PACEMAKER_AUTH_METHOD.");
            throw new RuntimeException("Can't start pacemaker server without proper PACEMAKER_AUTH_METHOD.");
        }

        ThreadFactory bossFactory = new NettyRenameThreadFactory("server-boss");
        ThreadFactory workerFactory = new NettyRenameThreadFactory("server-worker");
        NioServerSocketChannelFactory factory;
        if(maxWorkers > 0) {
            factory =
                new NioServerSocketChannelFactory(Executors.newCachedThreadPool(bossFactory),
                                                  Executors.newCachedThreadPool(workerFactory),
                                                  maxWorkers);
        }
        else {
            factory =
                new NioServerSocketChannelFactory(Executors.newCachedThreadPool(bossFactory),
                                                  Executors.newCachedThreadPool(workerFactory));
        }

        bootstrap = new ServerBootstrap(factory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("sendBufferSize", FIVE_MB_IN_BYTES);
        bootstrap.setOption("keepAlive", true);

        ChannelPipelineFactory pipelineFactory = new ThriftNettyServerCodec(this, config, authMethod).pipelineFactory();
        bootstrap.setPipelineFactory(pipelineFactory);
        Channel channel = bootstrap.bind(new InetSocketAddress(port));
        allChannels.add(channel);
        LOG.info("Bound server to port: {}", Integer.toString(port));
    }

    /** Implementing IServer. **/
    public void channelConnected(Channel c) {
        allChannels.add(c);
    }

    public void cleanPipeline(Channel channel) {
        boolean authenticated = authenticated_channels.contains(channel);
        if(!authenticated) {
            if(channel.getPipeline().get(ThriftNettyServerCodec.SASL_HANDLER) != null) {
                channel.getPipeline().remove(ThriftNettyServerCodec.SASL_HANDLER);
            }
            else if(channel.getPipeline().get(ThriftNettyServerCodec.KERBEROS_HANDLER) != null) {
                channel.getPipeline().remove(ThriftNettyServerCodec.KERBEROS_HANDLER);
            }
        }
    }

    public void received(Object mesg, String remote, Channel channel) throws InterruptedException {
        cleanPipeline(channel);

        boolean authenticated = (authMethod == ThriftNettyServerCodec.AuthMethod.NONE) || authenticated_channels.contains(channel);
        HBMessage m = (HBMessage)mesg;
        LOG.debug("received message. Passing to handler. {} : {} : {}",
                  handler.toString(), m.toString(), channel.toString());
        HBMessage response = handler.handleMessage(m, authenticated);
        if(response != null) {
            LOG.debug("Got Response from handler: {}", response);
            channel.write(response);
        }
        else {
            LOG.info("Got null response from handler handling message: {}", m);
        }
    }

    public void closeChannel(Channel c) {
        c.close().awaitUninterruptibly();
        allChannels.remove(c);
        authenticated_channels.remove(c);
    }

    public String name() {
        return topo_name;
    }

    public String secretKey() {
        return secret;
    }

    public void authenticated(Channel c) {
        LOG.debug("Pacemaker server authenticated channel: {}", c.toString());
        authenticated_channels.add(c);
    }
}
