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

import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.KerberosPrincipalToLocal;
import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import org.apache.zookeeper.server.auth.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class KerberosSaslNettyServer {

    private static final Logger LOG = LoggerFactory
        .getLogger(KerberosSaslNettyServer.class);

    private SaslServer saslServer;
    private Subject subject;
    private List<String> authorizedUsers;

    KerberosSaslNettyServer(Map storm_conf, String jaas_section, List<String> authorizedUsers) {
        this.authorizedUsers = authorizedUsers;
        LOG.debug("Getting Configuration.");
        Configuration login_conf;
        try {
            login_conf = AuthUtils.GetConfiguration(storm_conf);
        }
        catch (Throwable t) {
            LOG.error("Failed to get login_conf: ", t);
            throw t;
        }

        LOG.debug("KerberosSaslNettyServer: authmethod {}", SaslUtils.KERBEROS);

        KerberosSaslCallbackHandler ch = new KerberosSaslNettyServer.KerberosSaslCallbackHandler(authorizedUsers);

        //login our principal
        subject = null;
        try {
            LOG.debug("Setting Configuration to login_config: {}", login_conf);
            //specify a configuration object to be used
            Configuration.setConfiguration(login_conf);
            //now login
            LOG.debug("Trying to login.");
            Login login = new Login(jaas_section, ch);
            subject = login.getSubject();
            LOG.debug("Got Subject: {}", subject.toString());
        } catch (LoginException ex) {
            LOG.error("Server failed to login in principal:", ex);
            throw new RuntimeException(ex);
        }

        //check the credential of our principal
        if (subject.getPrivateCredentials(KerberosTicket.class).isEmpty()) {
            LOG.error("Failed to verifyuser principal.");
            throw new RuntimeException("Fail to verify user principal with section \""
                                       + jaas_section
                                       + "\" in login configuration file "
                                       + login_conf);
        }

        try {
            LOG.info("Creating Kerberos Server.");
            final CallbackHandler fch = ch;
            Principal p = (Principal)subject.getPrincipals().toArray()[0];
            KerberosName kName = new KerberosName(p.getName());
            final String fHost = kName.getHostName();
            final String fServiceName = kName.getServiceName();
            LOG.debug("Server with host: {}", fHost);
            saslServer =
                Subject.doAs(subject, new PrivilegedExceptionAction<SaslServer>() {
                        public SaslServer run() {
                            try {
                                Map<String, String> props = new TreeMap<String,String>();
                                props.put(Sasl.QOP, "auth");
                                props.put(Sasl.SERVER_AUTH, "false");
                                return Sasl.createSaslServer(SaslUtils.KERBEROS,
                                                             fServiceName,
                                                             fHost, props, fch);
                            }
                            catch (Exception e) {
                                LOG.error("Subject failed to create sasl server.", e);
                                return null;
                            }
                        }
                    });
            LOG.info("Got Server: {}", saslServer);

        } catch (PrivilegedActionException e) {
            LOG.error("KerberosSaslNettyServer: Could not create SaslServer: ", e);
            throw new RuntimeException(e);
        }
    }

    public boolean isComplete() {
        return saslServer.isComplete();
    }

    public String getUserName() {
        return saslServer.getAuthorizationID();
    }

    /** CallbackHandler for SASL DIGEST-MD5 mechanism */
    public static class KerberosSaslCallbackHandler implements CallbackHandler {

        /** Used to authenticate the clients */
        private List<String> authorizedUsers;

        public KerberosSaslCallbackHandler(List<String> authorizedUsers) {
            LOG.debug("KerberosSaslCallback: Creating KerberosSaslCallback handler.");
            this.authorizedUsers = authorizedUsers;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                LOG.info("Kerberos Callback Handler got callback: {}", callback.getClass());
                if(callback instanceof AuthorizeCallback) {
                    AuthorizeCallback ac = (AuthorizeCallback)callback;
                    if(!ac.getAuthenticationID().equals(ac.getAuthorizationID())) {
                        LOG.debug("{} != {}", ac.getAuthenticationID(), ac.getAuthorizationID());
                        continue;
                    }

                    LOG.debug("Authorized Users: {}", authorizedUsers);
                    LOG.debug("Checking authorization for: {}", ac.getAuthorizationID());
                    for(String user : authorizedUsers) {
                        String requester = ac.getAuthorizationID();

                        KerberosPrincipal principal = new KerberosPrincipal(requester);
                        requester = new KerberosPrincipalToLocal().toLocal(principal);

                        if(requester.equals(user) ) {
                            ac.setAuthorized(true);
                            break;
                        }
                    }
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
    public byte[] response(final byte[] token) {
        try {
            byte [] retval = Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
                    public byte[] run(){
                        try {
                            LOG.debug("response: Responding to input token of length: {}",
                                      token.length);
                            byte[] retval = saslServer.evaluateResponse(token);
                            return retval;
                        } catch (SaslException e) {
                            LOG.error("response: Failed to evaluate client token of length: {} : {}",
                                      token.length, e);
                            throw new RuntimeException(e);
                        }
                    }
                });
            return retval;
        }
        catch (PrivilegedActionException e) {
            LOG.error("Failed to generate response for token: ", e);
            throw new RuntimeException(e);
        }
    }
}
