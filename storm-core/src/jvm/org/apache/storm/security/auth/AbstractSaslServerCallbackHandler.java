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
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractSaslServerCallbackHandler implements CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSaslServerCallbackHandler.class);
    protected final Map<String,String> credentials = new HashMap<>();
    protected String userName;

    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                handleNameCallback((NameCallback) callback);
            } else if (callback instanceof PasswordCallback) {
                handlePasswordCallback((PasswordCallback) callback);
            } else if (callback instanceof RealmCallback) {
                handleRealmCallback((RealmCallback) callback);
            } else if (callback instanceof AuthorizeCallback) {
                handleAuthorizeCallback((AuthorizeCallback) callback);
            }
        }
    }

    private void handleNameCallback(NameCallback nc) {
        LOG.debug("handleNameCallback");
        userName = nc.getDefaultName();
        nc.setName(nc.getDefaultName());
    }

    protected void handlePasswordCallback(PasswordCallback pc) {
        LOG.debug("handlePasswordCallback");
        if (credentials.containsKey(userName) ) {
            pc.setPassword(credentials.get(userName).toCharArray());
        } else {
            LOG.warn("No password found for user: {}", userName);
        }
    }

    private void handleRealmCallback(RealmCallback rc) {
        LOG.debug("handleRealmCallback: {}", rc.getDefaultText());
        rc.setText(rc.getDefaultText());
    }

    private void handleAuthorizeCallback(AuthorizeCallback ac) {
        String authenticationID = ac.getAuthenticationID();
        LOG.info("Successfully authenticated client: authenticationID = {} authorizationID = {}",
            authenticationID, ac.getAuthorizationID());

        //if authorizationId is not set, set it to authenticationId.
        if(ac.getAuthorizationID() == null) {
            ac.setAuthorizedID(authenticationID);
        }

        //When authNid and authZid are not equal , authNId is attempting to impersonate authZid, We
        //add the authNid as the real user in reqContext's subject which will be used during authorization.
        if(!authenticationID.equals(ac.getAuthorizationID())) {
            LOG.info("Impersonation attempt  authenticationID = {} authorizationID = {}",
                ac.getAuthenticationID(),  ac.getAuthorizationID());
            ReqContext.context().setRealPrincipal(new SaslTransportPlugin.User(ac.getAuthenticationID()));
        } else {
            ReqContext.context().setRealPrincipal(null);
        }

        ac.setAuthorized(true);
    }
}
