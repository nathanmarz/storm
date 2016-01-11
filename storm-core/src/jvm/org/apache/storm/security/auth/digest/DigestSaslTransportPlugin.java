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
package org.apache.storm.security.auth.digest;

import java.io.IOException;

import javax.security.auth.callback.CallbackHandler;

import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.SaslTransportPlugin;

public class DigestSaslTransportPlugin extends SaslTransportPlugin {
    public static final String DIGEST = "DIGEST-MD5";
    private static final Logger LOG = LoggerFactory.getLogger(DigestSaslTransportPlugin.class);

    protected TTransportFactory getServerTransportFactory() throws IOException {        
        //create an authentication callback handler
        CallbackHandler serer_callback_handler = new ServerCallbackHandler(login_conf);

        //create a transport factory that will invoke our auth callback for digest
        TSaslServerTransport.Factory factory = new TSaslServerTransport.Factory();
        factory.addServerDefinition(DIGEST, AuthUtils.SERVICE, "localhost", null, serer_callback_handler);

        LOG.info("SASL DIGEST-MD5 transport factory will be used");
        return factory;
    }

    @Override
    public TTransport connect(TTransport transport, String serverHost, String asUser) throws TTransportException, IOException {
        ClientCallbackHandler client_callback_handler = new ClientCallbackHandler(login_conf);
        TSaslClientTransport wrapper_transport = new TSaslClientTransport(DIGEST,
                null,
                AuthUtils.SERVICE, 
                serverHost,
                null,
                client_callback_handler, 
                transport);

        wrapper_transport.open();
        LOG.debug("SASL DIGEST-MD5 client transport has been established");

        return wrapper_transport;
    }

}
