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

import org.apache.storm.Config;
import org.apache.storm.security.auth.AuthUtils;
import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.TreeMap;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import org.apache.zookeeper.server.auth.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements SASL logic for storm worker client processes.
 */
public class KerberosSaslNettyClient {

    private static final Logger LOG = LoggerFactory
        .getLogger(KerberosSaslNettyClient.class);

    /**
     * Used to respond to server's counterpart, SaslServer with SASL tokens
     * represented as byte arrays.
     */
    private SaslClient saslClient;
    private Subject subject;
    private String jaas_section;

    /**
     * Create a KerberosSaslNettyClient for authentication with servers.
     */
    public KerberosSaslNettyClient(Map storm_conf, String jaas_section) {
        LOG.debug("KerberosSaslNettyClient: Creating SASL {} client to authenticate to server ",
                  SaslUtils.KERBEROS);

        LOG.info("Creating Kerberos Client.");

        Configuration login_conf;
        try {
            login_conf = AuthUtils.GetConfiguration(storm_conf);
        }
        catch (Throwable t) {
            LOG.error("Failed to get login_conf: ", t);
            throw t;
        }
        LOG.debug("KerberosSaslNettyClient: authmethod {}", SaslUtils.KERBEROS);

        SaslClientCallbackHandler ch = new SaslClientCallbackHandler();

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
            LOG.error("Client failed to login in principal:" + ex, ex);
            throw new RuntimeException(ex);
        }

        //check the credential of our principal
        if (subject.getPrivateCredentials(KerberosTicket.class).isEmpty()) {
            LOG.error("Failed to verify user principal.");
            throw new RuntimeException("Fail to verify user principal with section \"" +
                                       jaas_section +
                                       "\" in login configuration file " +
                                       login_conf);
        }

        String serviceName = null;
        try {
            serviceName = AuthUtils.get(login_conf, jaas_section, "serviceName");
        }
        catch (IOException e) {
            LOG.error("Failed to get service name.", e);
            throw new RuntimeException(e);
        }

        try {
            Principal principal = (Principal)subject.getPrincipals().toArray()[0];
            final String fPrincipalName = principal.getName();
            final String fHost = (String)storm_conf.get(Config.PACEMAKER_HOST);
            final String fServiceName = serviceName;
            final CallbackHandler fch = ch;
            LOG.debug("Kerberos Client with principal: {}, host: {}", fPrincipalName, fHost);
            saslClient = Subject.doAs(subject, new PrivilegedExceptionAction<SaslClient>() {
                    public SaslClient run() {
                        try {
                            Map<String, String> props = new TreeMap<String,String>();
                            props.put(Sasl.QOP, "auth");
                            props.put(Sasl.SERVER_AUTH, "false");
                            return Sasl.createSaslClient(
                                new String[] { SaslUtils.KERBEROS },
                                fPrincipalName,
                                fServiceName,
                                fHost,
                                props, fch);
                        }
                        catch (Exception e) {
                            LOG.error("Subject failed to create sasl client.", e);
                            return null;
                        }
                    }
                });
            LOG.info("Got Client: {}", saslClient);

        } catch (PrivilegedActionException e) {
            LOG.error("KerberosSaslNettyClient: Could not create Sasl Netty Client.");
            throw new RuntimeException(e);
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
            final SaslMessageToken fSaslTokenMessage = saslTokenMessage;
            byte [] retval = Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
                    public byte[] run() {
                        try {
                            byte[] retval = saslClient.evaluateChallenge(fSaslTokenMessage
                                                                         .getSaslToken());
                            return retval;
                        } catch (SaslException e) {
                            LOG.error("saslResponse: Failed to respond to SASL server's token:",
                                      e);
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

    /**
     * Implementation of javax.security.auth.callback.CallbackHandler that works
     * with Storm topology tokens.
     */
    private static class SaslClientCallbackHandler implements CallbackHandler {

        /**
         * Implementation used to respond to SASL tokens from server.
         *
         * @param callbacks
         *            objects that indicate what credential information the
         *            server's SaslServer requires from the client.
         * @throws UnsupportedCallbackException
         */
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                LOG.info("Kerberos Client Callback Handler got callback: {}", callback.getClass());
            }
        }
    }

}
