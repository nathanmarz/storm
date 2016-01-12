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

import java.io.IOException;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.security.auth.AuthUtils;

/**
 * SASL client side callback handler.
 */
public class ClientCallbackHandler implements CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ClientCallbackHandler.class);

    /**
     * Constructor based on a JAAS configuration
     * 
     * For digest, you should have a pair of user name and password defined in this figgure.
     * 
     * @param configuration
     * @throws IOException
     */
    public ClientCallbackHandler(Configuration configuration) throws IOException {
        if (configuration == null) return;
        AppConfigurationEntry configurationEntries[] = configuration.getAppConfigurationEntry(AuthUtils.LOGIN_CONTEXT_CLIENT);
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '"+AuthUtils.LOGIN_CONTEXT_CLIENT
                    + "' entry in this configuration: Client cannot start.";
            LOG.error(errorMessage);
            throw new IOException(errorMessage);
        }
    }

    /**
     * This method is invoked by SASL for authentication challenges
     * @param callbacks a collection of challenge callbacks 
     */
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback c : callbacks) {
            if (c instanceof NameCallback) {
                LOG.debug("name callback");
            } else if (c instanceof PasswordCallback) {
                LOG.debug("password callback");
                LOG.warn("Could not login: the client is being asked for a password, but the " +
                        " client code does not currently support obtaining a password from the user." +
                        " Make sure that the client is configured to use a ticket cache (using" +
                        " the JAAS configuration setting 'useTicketCache=true)' and restart the client. If" +
                        " you still get this message after that, the TGT in the ticket cache has expired and must" +
                        " be manually refreshed. To do so, first determine if you are using a password or a" +
                        " keytab. If the former, run kinit in a Unix shell in the environment of the user who" +
                        " is running this client using the command" +
                        " 'kinit <princ>' (where <princ> is the name of the client's Kerberos principal)." +
                        " If the latter, do" +
                        " 'kinit -k -t <keytab> <princ>' (where <princ> is the name of the Kerberos principal, and" +
                        " <keytab> is the location of the keytab file). After manually refreshing your cache," +
                        " restart this client. If you continue to see this message after manually refreshing" +
                        " your cache, ensure that your KDC host's clock is in sync with this host's clock.");
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
            }  else {
                throw new UnsupportedCallbackException(c);
            }
        }
    }
}
