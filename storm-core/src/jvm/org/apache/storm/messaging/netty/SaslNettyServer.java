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
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SaslNettyServer {

    private static final Logger LOG = LoggerFactory
        .getLogger(SaslNettyServer.class);

        private SaslServer saslServer;

    SaslNettyServer(String topologyName, byte[] token) throws IOException {
        LOG.debug("SaslNettyServer: Topology token is: {} with authmethod {}",
                  topologyName, SaslUtils.AUTH_DIGEST_MD5);

        try {
            SaslDigestCallbackHandler ch = new SaslNettyServer.SaslDigestCallbackHandler(
                topologyName, token);

            saslServer = Sasl.createSaslServer(SaslUtils.AUTH_DIGEST_MD5, null,
                                               SaslUtils.DEFAULT_REALM,
                                               SaslUtils.getSaslProps(), ch);
        } catch (SaslException e) {
            LOG.error("SaslNettyServer: Could not create SaslServer: ", e);
        }
    }

    public boolean isComplete() {
        return saslServer.isComplete();
    }

    public String getUserName() {
        return saslServer.getAuthorizationID();
    }

    /** CallbackHandler for SASL DIGEST-MD5 mechanism */
    public static class SaslDigestCallbackHandler implements CallbackHandler {

        /** Used to authenticate the clients */
        private byte[] userPassword;
        private String userName;

        public SaslDigestCallbackHandler(String topologyName, byte[] token) {
            LOG.debug("SaslDigestCallback: Creating SaslDigestCallback handler with topology token: {}", topologyName);
            this.userName = topologyName;
            this.userPassword = token;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException,
            UnsupportedCallbackException {
            NameCallback nc = null;
            PasswordCallback pc = null;
            AuthorizeCallback ac = null;

            for (Callback callback : callbacks) {
                if (callback instanceof AuthorizeCallback) {
                    ac = (AuthorizeCallback) callback;
                } else if (callback instanceof NameCallback) {
                    nc = (NameCallback) callback;
                } else if (callback instanceof PasswordCallback) {
                    pc = (PasswordCallback) callback;
                } else if (callback instanceof RealmCallback) {
                    continue; // realm is ignored
                } else {
                    throw new UnsupportedCallbackException(callback,
                                                           "handle: Unrecognized SASL DIGEST-MD5 Callback");
                }
            }

            if (nc != null) {
                LOG.debug("handle: SASL server DIGEST-MD5 callback: setting username for client: {}",
                          userName);
                nc.setName(userName);
            }

            if (pc != null) {
                char[] password = SaslUtils.encodePassword(userPassword);

                LOG.debug("handle: SASL server DIGEST-MD5 callback: setting password for client: ",
                          userPassword);

                pc.setPassword(password);
            }
            if (ac != null) {

                String authid = ac.getAuthenticationID();
                String authzid = ac.getAuthorizationID();

                if (authid.equals(authzid)) {
                    ac.setAuthorized(true);
                } else {
                    ac.setAuthorized(false);
                }

                if (ac.isAuthorized()) {
                    LOG.debug("handle: SASL server DIGEST-MD5 callback: setting canonicalized client ID: ",
                              userName);
                    ac.setAuthorizedID(authzid);
                }
            }
        }
    }

    /**
     * Used by SaslTokenMessage::processToken() to respond to server SASL
     * tokens.
     *
     * @param token
     *            Server's SASL token
     * @return token to send back to the server.
     */
    public byte[] response(byte[] token) {
        try {
            LOG.debug("response: Responding to input token of length: {}",
                      token.length);
            byte[] retval = saslServer.evaluateResponse(token);
            LOG.debug("response: Response token length: {}", retval.length);
            return retval;
        } catch (SaslException e) {
            LOG.error("response: Failed to evaluate client token of length: {} : {}",
                      token.length, e);
            return null;
        }
    }
}
