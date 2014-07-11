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

package backtype.storm.security.auth.hadoop;

import backtype.storm.Config;
import backtype.storm.security.INimbusCredentialPlugin;
import backtype.storm.security.auth.IAutoCredentials;
import backtype.storm.security.auth.ICredentialsRenewer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.lang.reflect.Method;
import java.net.URI;
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

    public void prepare(Map conf) {
       //no op.
    }

    @SuppressWarnings("unchecked")
    private byte[] getHDFSCredsWithDelegationToken(Map conf) throws Exception {

        try {
             // What we want to do is following:
             //  if(UserGroupInformation.isSecurityEnabled) {
             //      FileSystem fs = FileSystem.get(nameNodeURI, configuration, topologySubmitterUser);
             //      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
             //      UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(topologySubmitterUser, ugi);
             //      Credentials credential= proxyUser.getCredentials();
             //      fs.addDelegationToken(hdfsUser, credential);
             // }
             // and then return the credential object as a bytearray.
             //
             // Following are the minimum set of configuration that needs to be set,  users should have hdfs-site.xml
             // and core-site.xml in the class path which should set these configuration.
             // configuration.set("hadoop.security.authentication", "KERBEROS");
             // configuration.set("dfs.namenode.kerberos.principal",
             //                                "hdfs/zookeeper.witzend.com@WITZEND.COM");
             // configuration.set("hadoop.security.kerberos.ticket.cache.path", "/tmp/krb5cc_1002");
             // and the ticket cache must have the hdfs user's creds.

            Class configurationClass = Class.forName("org.apache.hadoop.conf.Configuration");
            Object configuration = configurationClass.newInstance();

            //UserGroupInformation.isSecurityEnabled
            final Class ugiClass = Class.forName("org.apache.hadoop.security.UserGroupInformation");
            final Method isSecurityEnabledMethod = ugiClass.getDeclaredMethod("isSecurityEnabled");
            boolean isSecurityEnabled = (Boolean)isSecurityEnabledMethod.invoke(null);

            if(isSecurityEnabled) {
                final String topologySubmitterUser = (String) conf.get(Config.TOPOLOGY_SUBMITTER_USER);
                final String hdfsUser = (String) conf.get(Config.TOPOLOGY_HDFS_PRINCIPAL);

                //FileSystem fs = FileSystem.get(nameNodeURI, configuration, topologySubmitterUser);
                Class fileSystemClass = Class.forName("org.apache.hadoop.fs.FileSystem");

                Object nameNodeURI = conf.containsKey(Config.TOPOLOGY_HDFS_URI) ? conf.get(Config.TOPOLOGY_HDFS_URI)
                        : fileSystemClass.getMethod("getDefaultUri", configurationClass).invoke(null, configuration);
                Method getMethod = fileSystemClass.getMethod("get", URI.class, configurationClass, String.class);
                Object fileSystem = getMethod.invoke(null, nameNodeURI, configuration, topologySubmitterUser);

                //UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                Method getCurrentUserMethod = ugiClass.getMethod("getCurrentUser");
                final Object ugi = getCurrentUserMethod.invoke(null);

                //UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(topologySubmitterUser, ugi);
                Method createProxyUserMethod = ugiClass.getMethod("createProxyUser", String.class, ugiClass);
                Object proxyUGI = createProxyUserMethod.invoke(null, topologySubmitterUser, ugi);

                //Credentials credential= proxyUser.getCredentials();
                Method getCredentialsMethod = ugiClass.getMethod("getCredentials");
                Object credentials = getCredentialsMethod.invoke(proxyUGI);

                //fs.addDelegationToken(hdfsUser, credential);
                Class credentialClass = Class.forName("org.apache.hadoop.security.Credentials");
                Method addDelegationTokensMethod = fileSystemClass.getMethod("addDelegationTokens", String.class,
                        credentialClass);
                addDelegationTokensMethod.invoke(fileSystem, hdfsUser, credentials);


                ByteArrayOutputStream bao = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bao);
                Method writeMethod = credentialClass.getMethod("write", DataOutput.class);
                writeMethod.invoke(credentials, out);
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

    @Override
    public void populateCredentials(Map<String, String> credentials, Map conf) {
        if(conf.containsKey(Config.TOPOLOGY_HDFS_PRINCIPAL)) {
            try {
                credentials.put(HDFS_CREDENTIALS, DatatypeConverter.printBase64Binary(getHDFSCredsWithDelegationToken(conf)));
            } catch (Exception e) {
                LOG.warn("Could not populate HDFS credentials.", e);
            }
        }
    }

    @Override
    public void populateCredentials(Map<String, String> credentials) {
        //no op.
    }

    /*
     *
     * @param credentials map with creds.
     * @return instance of org.apache.hadoop.security.Credentials, if the Map has HDFS_CREDENTIALS.
     * this class's populateCredentials must have been called before.
     */
    @SuppressWarnings("unchecked")
    private static Object getHDFSCredential(Map<String, String> credentials) {
        Object credential = null;
        if (credentials != null && credentials.containsKey(HDFS_CREDENTIALS)) {
            try {
                byte[] credBytes = DatatypeConverter.parseBase64Binary(credentials.get(HDFS_CREDENTIALS));
                ByteArrayInputStream bai = new ByteArrayInputStream(credBytes);
                ObjectInputStream in = new ObjectInputStream(bai);

                Class credentialClass = Class.forName("org.apache.hadoop.security.Credentials");
                credential = credentialClass.newInstance();
                Method readMethod  = credentialClass.getMethod("readFields", DataInput.class);
                readMethod.invoke(credential, in);
            } catch (Exception e) {
                LOG.warn("Could not obtain HDFS credentials from credentials map.", e);
            }
        }
        return credential;
    }

    @Override
    public void updateSubject(Subject subject, Map<String, String> credentials) {
        addCredentialToSubject(subject, credentials);
    }

    @Override
    public void populateSubject(Subject subject, Map<String, String> credentials) {
        addCredentialToSubject(subject, credentials);
    }

    @SuppressWarnings("unchecked")
    private static void addCredentialToSubject(Subject subject, Map<String, String> credentials) {
        try {
            Object credential = getHDFSCredential(credentials);
            if (credential != null) {
                subject.getPrivateCredentials().add(credential);
            } else {
                LOG.info("No HDFS credential found in credentials");
            }
        } catch (Exception e) {
            LOG.warn("Failed to initialize and get UserGroupInformation.", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void renew(Map<String, String> credentials, Map topologyConf) {
        Object credential = getHDFSCredential(credentials);
        //maximum allowed expiration time until which tokens will keep renewing,
        //currently set to 1 day.
        final long MAX_ALLOWED_EXPIRATION_MILLIS = 24 * 60 * 60 * 1000;

        /**
         * We are trying to do the following :
         * List<Token> tokens = credential.getAllTokens();
         * for(Token token: tokens) {
         *      long expiration = token.renew(configuration);
         * }
         */
        try {
            if (credential != null) {
                Class configurationClass = Class.forName("org.apache.hadoop.conf.Configuration");
                Object configuration = configurationClass.newInstance();

                Class credentialClass = Class.forName("org.apache.hadoop.security.Credentials");
                Class tokenClass = Class.forName("org.apache.hadoop.security.token.Token");

                Method renewMethod = tokenClass.getMethod("renew", configurationClass);
                Method getAllTokensMethod = credentialClass.getMethod("getAllTokens");

                Collection<?> tokens = (Collection<?>) getAllTokensMethod.invoke(credential);

                for (Object token : tokens) {
                    long expiration = (Long) renewMethod.invoke(token, configuration);
                    if(expiration < MAX_ALLOWED_EXPIRATION_MILLIS) {
                        LOG.debug("expiration {} is less then MAX_ALLOWED_EXPIRATION_MILLIS {}, getting new tokens",
                                expiration, MAX_ALLOWED_EXPIRATION_MILLIS);
                        populateCredentials(credentials, topologyConf);
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("could not renew the credentials, one of the possible reason is tokens are beyond " +
                    "renewal period so attempting to get new tokens.", e);
            populateCredentials(credentials);
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map conf = new java.util.HashMap();
        conf.put(Config.TOPOLOGY_SUBMITTER_PRINCIPAL, args[0]); //with realm e.g. storm@WITZEND.COM
        conf.put(Config.TOPOLOGY_HDFS_PRINCIPAL, args[1]); //with realm e.g. hdfs@WITZEND.COM

        AutoHDFS autoHDFS = new AutoHDFS();
        autoHDFS.prepare(conf);

        Map<String,String> creds  = new HashMap<String, String>();
        autoHDFS.populateCredentials(creds, conf);
        LOG.info("Got HDFS credentials", AutoHDFS.getHDFSCredential(creds));

        Subject s = new Subject();
        autoHDFS.populateSubject(s, creds);
        LOG.info("Got a Subject "+ s);

        autoHDFS.renew(creds, conf);
        LOG.info("renewed credentials", AutoHDFS.getHDFSCredential(creds));
    }

    @Override
    public void shutdown() {

    }
}

