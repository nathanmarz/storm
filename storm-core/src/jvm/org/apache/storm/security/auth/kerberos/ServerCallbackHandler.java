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

package org.apache.storm.security.auth.kerberos;

import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.SaslTransportPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.sasl.AuthorizeCallback;
import java.io.IOException;
import java.util.Map;

/**
 * SASL server side callback handler
 */
public class ServerCallbackHandler implements CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ServerCallbackHandler.class);

    private String userName;

    public ServerCallbackHandler(Configuration configuration, Map stormConf) throws IOException {
        if (configuration==null) return;

        AppConfigurationEntry configurationEntries[] = configuration.getAppConfigurationEntry(AuthUtils.LOGIN_CONTEXT_SERVER);
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '"+AuthUtils.LOGIN_CONTEXT_SERVER+"' entry in this configuration: Server cannot start.";
            LOG.error(errorMessage);
            throw new IOException(errorMessage);
        }

    }

    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                handleNameCallback((NameCallback) callback);
            } else if (callback instanceof PasswordCallback) {
                handlePasswordCallback((PasswordCallback) callback);
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

    private void handlePasswordCallback(PasswordCallback pc) {
        LOG.warn("No password found for user: " + userName);
    }

    private void handleAuthorizeCallback(AuthorizeCallback ac) {
        String authenticationID = ac.getAuthenticationID();
        LOG.info("Successfully authenticated client: authenticationID=" + authenticationID + " authorizationID= " + ac.getAuthorizationID());

        //if authorizationId is not set, set it to authenticationId.
        if(ac.getAuthorizationID() == null) {
            ac.setAuthorizedID(authenticationID);
        }

        //When authNid and authZid are not equal , authNId is attempting to impersonate authZid, We
        //add the authNid as the real user in reqContext's subject which will be used during authorization.
        if(!ac.getAuthenticationID().equals(ac.getAuthorizationID())) {
            ReqContext.context().setRealPrincipal(new SaslTransportPlugin.User(ac.getAuthenticationID()));
        } else {
            ReqContext.context().setRealPrincipal(null);
        }

        ac.setAuthorized(true);
    }
}
