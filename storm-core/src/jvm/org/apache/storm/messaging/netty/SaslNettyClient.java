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

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements SASL logic for storm worker client processes.
 */
public class SaslNettyClient {

    private static final Logger LOG = LoggerFactory
            .getLogger(SaslNettyClient.class);

    /**
     * Used to respond to server's counterpart, SaslServer with SASL tokens
     * represented as byte arrays.
     */
    private SaslClient saslClient;

    /**
     * Create a SaslNettyClient for authentication with servers.
     */
    public SaslNettyClient(String topologyName, byte[] token) {
        try {
            LOG.debug("SaslNettyClient: Creating SASL {} client to authenticate to server ",
                      SaslUtils.AUTH_DIGEST_MD5);

            saslClient = Sasl.createSaslClient(
                    new String[] { SaslUtils.AUTH_DIGEST_MD5 }, null, null,
                    SaslUtils.DEFAULT_REALM, SaslUtils.getSaslProps(),
                    new SaslClientCallbackHandler(topologyName, token));

        } catch (IOException e) {
            LOG.error("SaslNettyClient: Could not obtain topology token for Netty "
                    + "Client to use to authenticate with a Netty Server.");
            saslClient = null;
        }
    }

    public boolean isComplete() {
        return saslClient.isComplete();
    }

    /**
     * Respond to server's SASL token.
     * 
     * @param saslTokenMessage
     *            contains server's SASL token
     * @return client's response SASL token
     */
    public byte[] saslResponse(SaslMessageToken saslTokenMessage) {
        try {
            return saslClient.evaluateChallenge(saslTokenMessage.getSaslToken());
        } catch (SaslException e) {
            LOG.error(
                    "saslResponse: Failed to respond to SASL server's token:",
                    e);
            return null;
        }
    }

    /**
     * Implementation of javax.security.auth.callback.CallbackHandler that works
     * with Storm topology tokens.
     */
    private static class SaslClientCallbackHandler implements CallbackHandler {
        /** Generated username contained in TopologyToken */
        private final String userName;
        /** Generated password contained in TopologyToken */
        private final char[] userPassword;

        /**
         * Set private members using topology token.
         */
        public SaslClientCallbackHandler(String topologyToken, byte[] token) {
            this.userName = SaslUtils
                    .encodeIdentifier(topologyToken.getBytes());
            this.userPassword = SaslUtils.encodePassword(token);
        }

        /**
         * Implementation used to respond to SASL tokens from server.
         * 
         * @param callbacks
         *            objects that indicate what credential information the
         *            server's SaslServer requires from the client.
         * @throws UnsupportedCallbackException
         */
        public void handle(Callback[] callbacks)
                throws UnsupportedCallbackException {
            NameCallback nc = null;
            PasswordCallback pc = null;
            RealmCallback rc = null;
            for (Callback callback : callbacks) {
                if (callback instanceof RealmChoiceCallback) {
                    continue;
                } else if (callback instanceof NameCallback) {
                    nc = (NameCallback) callback;
                } else if (callback instanceof PasswordCallback) {
                    pc = (PasswordCallback) callback;
                } else if (callback instanceof RealmCallback) {
                    rc = (RealmCallback) callback;
                } else {
                    throw new UnsupportedCallbackException(callback,
                            "handle: Unrecognized SASL client callback");
                }
            }
            if (nc != null) {
                LOG.debug("handle: SASL client callback: setting username: {}",
                          userName);
                nc.setName(userName);
            }
            if (pc != null) {
                LOG.debug("handle: SASL client callback: setting userPassword");
                pc.setPassword(userPassword);
            }
            if (rc != null) {
                LOG.debug("handle: SASL client callback: setting realm: {}",
                        rc.getDefaultText());
                rc.setText(rc.getDefaultText());
            }
        }
    }
}
