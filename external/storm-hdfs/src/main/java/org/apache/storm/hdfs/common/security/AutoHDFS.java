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

package org.apache.storm.hdfs.common.security;

import org.apache.storm.Config;
import org.apache.storm.security.INimbusCredentialPlugin;
import org.apache.storm.security.auth.IAutoCredentials;
import org.apache.storm.security.auth.ICredentialsRenewer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.storm.hdfs.common.security.HdfsSecurityUtil.STORM_KEYTAB_FILE_KEY;
import static org.apache.storm.hdfs.common.security.HdfsSecurityUtil.STORM_USER_NAME_KEY;

/**
 * Automatically get HDFS delegation tokens and push it to user's topology. The class
 * assumes that HDFS configuration files are in your class path.
 */
public class AutoHDFS implements IAutoCredentials, ICredentialsRenewer, INimbusCredentialPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(AutoHDFS.class);
    public static final String HDFS_CREDENTIALS = "HDFS_CREDENTIALS";
    public static final String TOPOLOGY_HDFS_URI = "topology.hdfs.uri";

    private String hdfsKeyTab;
    private String hdfsPrincipal;

    @Override
    public void prepare(Map conf) {
        if(conf.containsKey(STORM_KEYTAB_FILE_KEY) && conf.containsKey(STORM_USER_NAME_KEY)) {
            this.hdfsKeyTab = (String) conf.get(STORM_KEYTAB_FILE_KEY);
            this.hdfsPrincipal = (String) conf.get(STORM_USER_NAME_KEY);
        }
    }

    @Override
    public void shutdown() {
        //no op.
    }

    @Override
    public void populateCredentials(Map<String, String> credentials, Map conf) {
        try {
            credentials.put(getCredentialKey(), DatatypeConverter.printBase64Binary(getHadoopCredentials(conf)));
            LOG.info("HDFS tokens added to credentials map.");
        } catch (Exception e) {
            LOG.error("Could not populate HDFS credentials.", e);
        }
    }

    @Override
    public void populateCredentials(Map<String, String> credentials) {
        credentials.put(HDFS_CREDENTIALS, DatatypeConverter.printBase64Binary("dummy place holder".getBytes()));
    }

    /*
 *
 * @param credentials map with creds.
 * @return instance of org.apache.hadoop.security.Credentials.
 * this class's populateCredentials must have been called before.
 */
    @SuppressWarnings("unchecked")
    protected Credentials getCredentials(Map<String, String> credentials) {
        Credentials credential = null;
        if (credentials != null && credentials.containsKey(getCredentialKey())) {
            try {
                byte[] credBytes = DatatypeConverter.parseBase64Binary(credentials.get(getCredentialKey()));
                ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(credBytes));

                credential = new Credentials();
                credential.readFields(in);
            } catch (Exception e) {
                LOG.error("Could not obtain credentials from credentials map.", e);
            }
        }
        return credential;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateSubject(Subject subject, Map<String, String> credentials) {
        addCredentialToSubject(subject, credentials);
        addTokensToUGI(subject);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void populateSubject(Subject subject, Map<String, String> credentials) {
        addCredentialToSubject(subject, credentials);
        addTokensToUGI(subject);
    }

    @SuppressWarnings("unchecked")
    private void addCredentialToSubject(Subject subject, Map<String, String> credentials) {
        try {
            Credentials credential = getCredentials(credentials);
            if (credential != null) {
                subject.getPrivateCredentials().add(credential);
                LOG.info("HDFS Credentials added to the subject.");
            } else {
                LOG.info("No credential found in credentials");
            }
        } catch (Exception e) {
            LOG.error("Failed to initialize and get UserGroupInformation.", e);
        }
    }

    public void addTokensToUGI(Subject subject) {
        if(subject != null) {
            Set<Credentials> privateCredentials = subject.getPrivateCredentials(Credentials.class);
            if (privateCredentials != null) {
                for (Credentials cred : privateCredentials) {
                    Collection<Token<? extends TokenIdentifier>> allTokens = cred.getAllTokens();
                    if (allTokens != null) {
                        for (Token<? extends TokenIdentifier> token : allTokens) {
                            try {
                                UserGroupInformation.getCurrentUser().addToken(token);
                                LOG.info("Added delegation tokens to UGI.");
                            } catch (IOException e) {
                                LOG.error("Exception while trying to add tokens to ugi", e);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public void renew(Map<String, String> credentials, Map topologyConf) {
        try {
            Credentials credential = getCredentials(credentials);
            if (credential != null) {
                Configuration configuration = new Configuration();
                Collection<Token<? extends TokenIdentifier>> tokens = credential.getAllTokens();

                if(tokens != null && tokens.isEmpty() == false) {
                    for (Token token : tokens) {
                        //We need to re-login some other thread might have logged into hadoop using
                        // their credentials (e.g. AutoHBase might be also part of nimbu auto creds)
                        login(configuration);
                        long expiration = (Long) token.renew(configuration);
                        LOG.info("HDFS delegation token renewed, new expiration time {}", expiration);
                    }
                } else {
                    LOG.debug("No tokens found for credentials, skipping renewal.");
                }
            }
        } catch (Exception e) {
            LOG.warn("could not renew the credentials, one of the possible reason is tokens are beyond " +
                    "renewal period so attempting to get new tokens.", e);
            populateCredentials(credentials, topologyConf);
        }
    }

    @SuppressWarnings("unchecked")
    protected byte[] getHadoopCredentials(Map conf) {
        try {
            if(UserGroupInformation.isSecurityEnabled()) {
                final Configuration configuration = new Configuration();

                login(configuration);

                final String topologySubmitterUser = (String) conf.get(Config.TOPOLOGY_SUBMITTER_PRINCIPAL);

                final URI nameNodeURI = conf.containsKey(TOPOLOGY_HDFS_URI) ? new URI(conf.get(TOPOLOGY_HDFS_URI).toString())
                        : FileSystem.getDefaultUri(configuration);

                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

                final UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(topologySubmitterUser, ugi);

                Credentials creds = (Credentials) proxyUser.doAs(new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            FileSystem fileSystem = FileSystem.get(nameNodeURI, configuration);
                            Credentials credential= proxyUser.getCredentials();

                            fileSystem.addDelegationTokens(hdfsPrincipal, credential);
                            LOG.info("Delegation tokens acquired for user {}", topologySubmitterUser);
                            return credential;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });


                ByteArrayOutputStream bao = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bao);

                creds.write(out);
                out.flush();
                out.close();

                return bao.toByteArray();
            } else {
                throw new RuntimeException("Security is not enabled for HDFS");
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get delegation tokens." , ex);
        }
    }

    private void login(Configuration configuration) throws IOException {
        configuration.set(STORM_KEYTAB_FILE_KEY, this.hdfsKeyTab);
        configuration.set(STORM_USER_NAME_KEY, this.hdfsPrincipal);
        SecurityUtil.login(configuration, STORM_KEYTAB_FILE_KEY, STORM_USER_NAME_KEY);

        LOG.info("Logged into hdfs with principal {}", this.hdfsPrincipal);
    }

    protected String getCredentialKey() {
        return HDFS_CREDENTIALS;
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map conf = new HashMap();
        conf.put(Config.TOPOLOGY_SUBMITTER_PRINCIPAL, args[0]); //with realm e.g. storm@WITZEND.COM
        conf.put(STORM_USER_NAME_KEY, args[1]); //with realm e.g. hdfs@WITZEND.COM
        conf.put(STORM_KEYTAB_FILE_KEY, args[2]);// /etc/security/keytabs/storm.keytab

        Configuration configuration = new Configuration();
        AutoHDFS autoHDFS = new AutoHDFS();
        autoHDFS.prepare(conf);

        Map<String,String> creds  = new HashMap<String, String>();
        autoHDFS.populateCredentials(creds, conf);
        LOG.info("Got HDFS credentials", autoHDFS.getCredentials(creds));

        Subject s = new Subject();
        autoHDFS.populateSubject(s, creds);
        LOG.info("Got a Subject "+ s);

        autoHDFS.renew(creds, conf);
        LOG.info("renewed credentials", autoHDFS.getCredentials(creds));
    }
}

