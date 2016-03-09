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
package org.apache.storm.security.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import java.io.IOException;

public abstract class AbstractSaslClientCallbackHandler implements CallbackHandler {
    protected static final String USERNAME = "username";
    protected static final String PASSWORD = "password";
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSaslClientCallbackHandler.class);
    protected String _username = null;
    protected String _password = null;

    /**
     * This method is invoked by SASL for authentication challenges
     * @param callbacks a collection of challenge callbacks
     */
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback c : callbacks) {
            if (c instanceof NameCallback) {
                LOG.debug("name callback");
                NameCallback nc = (NameCallback) c;
                nc.setName(_username);
            } else if (c instanceof PasswordCallback) {
                LOG.debug("password callback");
                PasswordCallback pc = (PasswordCallback)c;
                if (_password != null) {
                    pc.setPassword(_password.toCharArray());
                }
            } else if (c instanceof AuthorizeCallback) {
                LOG.debug("authorization callback");
                AuthorizeCallback ac = (AuthorizeCallback) c;
                String authid = ac.getAuthenticationID();
                String authzid = ac.getAuthorizationID();
                if (authid.equals(authzid)) {
                    ac.setAuthorized(true);
                } else {
                    ac.setAuthorized(false);
                }
                if (ac.isAuthorized()) {
                    ac.setAuthorizedID(authzid);
                }
            } else if (c instanceof RealmCallback) {
                RealmCallback rc = (RealmCallback) c;
                ((RealmCallback) c).setText(rc.getDefaultText());
            } else {
                throw new UnsupportedCallbackException(c);
            }
        }
    }
}
