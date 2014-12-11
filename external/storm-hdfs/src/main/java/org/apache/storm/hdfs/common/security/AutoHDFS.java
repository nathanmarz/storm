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

import backtype.storm.Config;
import backtype.storm.security.INimbusCredentialPlugin;
import backtype.storm.security.auth.IAutoCredentials;
import backtype.storm.security.auth.ICredentialsRenewer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
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

/**
 * Automatically get HDFS delegation tokens and push it to user's topology. The class
 * assumes that HDFS configuration files are in your class path.
 */
public class AutoHDFS implements IAutoCredentials, ICredentialsRenewer, INimbusCredentialPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(AutoHDFS.class);
    public static final String HDFS_CREDENTIALS = "HDFS_CREDENTIALS";
    public static final String TOPOLOGY_HDFS_URI = "topology.hdfs.uri";

    @Override
    public void prepare(Map conf) {
        //no op.
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
            LOG.warn("Could not populate HDFS credentials.", e);
        }
    }

    @Override
    public void populateCredentials(Map<String, String> credentials) {
        //no op.
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
                LOG.warn("Could not obtain credentials from credentials map.", e);
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
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void populateSubject(Subject subject, Map<String, String> credentials) {
        addCredentialToSubject(subject, credentials);
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
            LOG.warn("Failed to initialize and get UserGroupInformation.", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public void renew(Map<String, String> credentials, Map topologyConf) {
        Credentials credential = getCredentials(credentials);
        //maximum allowed expiration time until which tokens will keep renewing,
        //currently set to 1 day.
        final long MAX_ALLOWED_EXPIRATION_MILLIS = 24 * 60 * 60 * 1000;

        try {
            if (credential != null) {
                Configuration configuration = new Configuration();
                Collection<Token<? extends TokenIdentifier>> tokens = credential.getAllTokens();

                if(tokens != null && tokens.isEmpty() == false) {
                    for (Token token : tokens) {
                        long expiration = (Long) token.renew(configuration);
                        if (expiration < MAX_ALLOWED_EXPIRATION_MILLIS) {
                            LOG.debug("expiration {} is less then MAX_ALLOWED_EXPIRATION_MILLIS {}, getting new tokens",
                                    expiration, MAX_ALLOWED_EXPIRATION_MILLIS);
                            populateCredentials(credentials, topologyConf);
                        }
                    }
                } else {
                    LOG.debug("No tokens found for credentials, skipping renewal.");
                }
            }
        } catch (Exception e) {
            LOG.warn("could not renew the credentials, one of the possible reason is tokens are beyond " +
                    "renewal period so attempting to get new tokens.", e);
            populateCredentials(credentials);
        }
    }

    @SuppressWarnings("unchecked")
    protected byte[] getHadoopCredentials(Map conf) {

        try {
            if(UserGroupInformation.isSecurityEnabled()) {
                final Configuration configuration = new Configuration();
                HdfsSecurityUtil.login(conf, configuration);

                final String topologySubmitterUser = (String) conf.get(Config.TOPOLOGY_SUBMITTER_PRINCIPAL);
                final String hdfsUser = (String) conf.get(HdfsSecurityUtil.STORM_USER_NAME_KEY);

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

                            fileSystem.addDelegationTokens(hdfsUser, credential);
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

    protected String getCredentialKey() {
        return HDFS_CREDENTIALS;
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map conf = new HashMap();
        conf.put(Config.TOPOLOGY_SUBMITTER_PRINCIPAL, args[0]); //with realm e.g. storm@WITZEND.COM
        conf.put(HdfsSecurityUtil.STORM_USER_NAME_KEY, args[1]); //with realm e.g. hdfs@WITZEND.COM
        conf.put(HdfsSecurityUtil.STORM_KEYTAB_FILE_KEY, args[2]);// /etc/security/keytabs/storm.keytab

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

