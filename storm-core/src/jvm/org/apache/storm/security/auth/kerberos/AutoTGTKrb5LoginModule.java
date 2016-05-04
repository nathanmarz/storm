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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.security.auth.AuthUtils;
import java.security.Principal;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;


/**
 * Custom LoginModule to enable Auto Login based on cached ticket
 */
public class AutoTGTKrb5LoginModule implements LoginModule {
    private static final Logger LOG = LoggerFactory.getLogger(AutoTGTKrb5LoginModule.class);

    // initial state
    private Subject subject;

    protected KerberosTicket kerbTicket = null;

    public void initialize(Subject subject,
                           CallbackHandler callbackHandler,
                           Map<String, ?> sharedState,
                           Map<String, ?> options) {

        this.subject = subject;
    }

    public boolean login() throws LoginException {
        LOG.debug("Acquire TGT from Cache");
        getKerbTicketFromCache();
        if (kerbTicket != null) {
            return true;
        } else {
            throw new LoginException("Authentication failed, the TGT not found.");
        }
    }

    protected void getKerbTicketFromCache() {
        kerbTicket = AutoTGT.kerbTicket.get();
    }

    protected Principal getKerbTicketClient() {
        if (kerbTicket != null) {
            return kerbTicket.getClient();
        }
        return null;
    }

    public boolean commit() throws LoginException {
        if (isSucceeded() == false) {
            return false;
        }
        if (subject == null || subject.isReadOnly()) {
            kerbTicket = null;
            throw new LoginException("Authentication failed because the Subject is invalid.");
        }
        // Let us add the kerbClientPrinc and kerbTicket
        // We need to clone the ticket because java.security.auth.kerberos assumes TGT is unique for each subject
        // So, sharing TGT with multiple subjects can cause expired TGT to never refresh.
        KerberosTicket kerbTicketCopy = AuthUtils.cloneKerberosTicket(kerbTicket);
        subject.getPrivateCredentials().add(kerbTicketCopy);
        subject.getPrincipals().add(getKerbTicketClient());
        LOG.debug("Commit Succeeded.");
        return true;
    }

    public boolean abort() throws LoginException {
        if (isSucceeded() == false) {
            return false;
        } else {
            return logout();
        }
    }

    public boolean logout() throws LoginException {
        if (subject != null && !subject.isReadOnly() && kerbTicket != null) {
            subject.getPrincipals().remove(kerbTicket.getClient());
            AutoTGT.clearCredentials(subject, null);
        }
        kerbTicket = null;
        return true;
    }

    private boolean isSucceeded() {
        return kerbTicket != null;
    }
}
