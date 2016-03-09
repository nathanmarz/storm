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

import org.apache.storm.security.auth.AbstractSaslClientCallbackHandler;
import org.apache.storm.security.auth.AuthUtils;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.io.IOException;

/**
 *  client side callback handler.
 */
public class ClientCallbackHandler extends AbstractSaslClientCallbackHandler {

    /**
     * Constructor based on a JAAS configuration
     * 
     * For digest, you should have a pair of user name and password defined.
     * @throws IOException
     */
    public ClientCallbackHandler(Configuration configuration) throws IOException {
        if (configuration == null) return;
        AppConfigurationEntry configurationEntries[] = configuration.getAppConfigurationEntry(AuthUtils.LOGIN_CONTEXT_CLIENT);
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '"+AuthUtils.LOGIN_CONTEXT_CLIENT
                    + "' entry in this configuration: Client cannot start.";
            throw new IOException(errorMessage);
        }

        _password = "";
        for(AppConfigurationEntry entry: configurationEntries) {
            if (entry.getOptions().get(USERNAME) != null) {
                _username = (String)entry.getOptions().get(USERNAME);
            }
            if (entry.getOptions().get(PASSWORD) != null) {
                _password = (String)entry.getOptions().get(PASSWORD);
            }
        }
    }

}
