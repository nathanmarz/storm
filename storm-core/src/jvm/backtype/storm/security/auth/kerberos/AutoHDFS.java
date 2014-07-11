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

package backtype.storm.security.auth.kerberos;

import backtype.storm.Config;
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
import java.util.Map;

/**
 * Automatically get HDFS delegation tokens and push it to user's topology. The class
 * assumes that HDFS configuration files are in your class path.
 */
public class AutoHDFS implements IAutoCredentials, ICredentialsRenewer {
    private static final Logger LOG = LoggerFactory.getLogger(AutoHDFS.class);
    public static final String HDFS_CREDENTIALS = "HDFS_CREDENTIALS";
    private static final String CONF_KEYTAB_KEY = "keytab";
    private static final String CONF_USER_KEY = "user";

    private Map conf;

    public void prepare(Map conf) {
        this.conf = conf;
    }

    @SuppressWarnings("unchecked")
    private Object getConfiguration() {
        try {
            final String hdfsUser = (String) conf.get(Config.HDFS_USER);
            final String hdfsUserKeyTab = (String) conf.get(Config.HDFS_USER_KEYTAB);

            /**
             *  Configuration configuration = new Configuration();
             *  configuration.set(CONF_KEYTAB_KEY, hdfsUserKeyTab);
             *  configuration.set(CONF_USER_KEY, hdfsUser);
             */
            Class configurationClass = Class.forName("org.apache.hadoop.conf.Configuration");
            Object configuration = configurationClass.newInstance();

            Method setMethod = configurationClass.getMethod("set", String.class, String.class);
            setMethod.invoke(configuration, CONF_KEYTAB_KEY, hdfsUserKeyTab);
            setMethod.invoke(configuration, CONF_USER_KEY, hdfsUser);
            /**
             * Following are the minimum set of configuration that needs to be set,  users should have hdfs-site.xml
             * and core-site.xml in the class path which should set these configuration.
             * setMethod.invoke(configuration, "hadoop.security.authentication", "KERBEROS");
             * setMethod.invoke(configuration,"dfs.namenode.kerberos.principal",
             *                                "hdfs/zookeeper.witzend.com@WITZEND.COM");
             * setMethod.invoke(configuration, "hadoop.security.kerberos.ticket.cache.path", "/tmp/krb5cc_1002");
             */

            setMethod.invoke(configuration, "hadoop.security.authentication", "KERBEROS");
            setMethod.invoke(configuration, "dfs.namenode.kerberos.principal","hdfs/zookeeper.witzend.com@WITZEND.COM");
            setMethod.invoke(configuration, "hadoop.security.kerberos.ticket.cache.path", "/tmp/krb5cc_1002");

            //UserGroupInformation.setConfiguration(configuration);
            final Class ugiClass = Class.forName("org.apache.hadoop.security.UserGroupInformation");
            Method setConfigurationMethod = ugiClass.getMethod("setConfiguration", configurationClass);
            setConfigurationMethod.invoke(null, configuration);
            return configuration;
        }  catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private void login(Object configuration) {
        try {
            Class configurationClass = Class.forName("org.apache.hadoop.conf.Configuration");
            final Class securityUtilClass = Class.forName("org.apache.hadoop.security.SecurityUtil");
            Method loginMethod = securityUtilClass.getMethod("login", configurationClass, String.class, String.class);
            loginMethod.invoke(null, configuration, CONF_KEYTAB_KEY, CONF_USER_KEY);
        } catch (Exception e) {
           throw new RuntimeException("Failed to login to hdfs .", e);
        }
    }

    @SuppressWarnings("unchecked")
    private byte[] getHDFSCredsWithDelegationToken() throws Exception {

        try {
            /**
             * What we want to do is following:
             *  Configuration configuration = new Configuration();
             *  configuration.set(CONF_KEYTAB_KEY, hdfsUserKeyTab);
             *  configuration.set(CONF_USER_KEY, hdfsUser);
             *  UserGroupInformation.setConfiguration(configuration);
             *  if(UserGroupInformation.isSecurityEnabled) {
             *      SecurityUtil.login(configuration, CONF_KEYTAB_KEY, CONF_USER_KEY);
             *      FileSystem fs = FileSystem.get(nameNodeURI, configuration, topologySubmitterUser);
             *      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
             *      UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(topologySubmitterUser, ugi);
             *      Credentials credential= proxyUser.getCredentials();
             *      fs.addDelegationToken(hdfsUser, credential);
             * }
             * and then return the credential object as a bytearray.
             */
            Object configuration = getConfiguration();
            final Class ugiClass = Class.forName("org.apache.hadoop.security.UserGroupInformation");
            final Method isSecurityEnabledMethod = ugiClass.getDeclaredMethod("isSecurityEnabled");
            boolean isSecurityEnabled = (Boolean)isSecurityEnabledMethod.invoke(null);
            if(isSecurityEnabled) {
                login(configuration);

                final URI nameNodeURI = URI.create((String) conf.get(Config.HDFS_NAMENODE_URL));
                final String topologySubmitterUser = (String) conf.get(Config.TOPOLOGY_SUBMITTER_USER);
                final String hdfsUser = (String) conf.get(Config.HDFS_USER);

                Class configurationClass = Class.forName("org.apache.hadoop.conf.Configuration");

                //FileSystem fs = FileSystem.get(nameNodeURI, configuration, topologySubmitterUser);
                Class fileSystemClass = Class.forName("org.apache.hadoop.fs.FileSystem");
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

                LOG.info(bao.toString());
                return bao.toByteArray();
            } else {
                throw new RuntimeException("Security is not enabled for HDFS");
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get delegation tokens." , ex);
        }
    }

    @Override
    public void populateCredentials(Map<String, String> credentials) {
        try {
            credentials.put(HDFS_CREDENTIALS, DatatypeConverter.printBase64Binary( getHDFSCredsWithDelegationToken()));
        } catch (Exception e) {
            LOG.warn("Could not populate HDFS credentials.", e);
        }
    }

    /**
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
    public void renew(Map<String,String> credentials) {
        Object credential = getHDFSCredential(credentials);
        /**
         * We are trying to do the following :
         * List<Token> tokens = credential.getAllTokens();
         * for(Token token: tokens) {
         *      token.renew(configuration);
         * }
         * TODO: Need to talk to HDFS guys to check what is recommended way to identify a token that is beyond
         * renew cycle, once we can identified tokens are not renewable we can repopulate credentials by asking HDFS
         * to issue new tokens. Until we have a better way any exception during renewal would result in attempt to
         * get new delegation tokens.
         */
        if (credential != null) {
            try {
                Object configuration = getConfiguration();

                Class configurationClass = Class.forName("org.apache.hadoop.conf.Configuration");
                Class credentialClass = Class.forName("org.apache.hadoop.security.Credentials");
                Class tokenClass = Class.forName("org.apache.hadoop.security.token.Token");

                Method renewMethod  = tokenClass.getMethod("renew", configurationClass);
                Method getAllTokensMethod = credentialClass.getMethod("getAllTokens");

                Collection<?> tokens = (Collection<?>) getAllTokensMethod.invoke(credential);

                for(Object token : tokens) {
                    renewMethod.invoke(token, configuration);
                }
            } catch(Exception e) {
                LOG.warn("could not renew the credentials, one of the possible reason is tokens are beyond " +
                        "renewal period so attempting to get new tokens.", e);
                populateCredentials(credentials);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map conf = new java.util.HashMap();
        conf.put(Config.HDFS_NAMENODE_URL, args[0]);
        conf.put(Config.TOPOLOGY_SUBMITTER_USER, args[1]); //with realm e.g. storm@WITZEND.COM
        conf.put(Config.HDFS_USER, args[2]); //with realm e.g. hdfs@WITZEND.COM
        conf.put(Config.HDFS_USER_KEYTAB, args[3]);

        AutoHDFS autoHDFS = new AutoHDFS();
        autoHDFS.prepare(conf);

        Map<String,String> creds = new java.util.HashMap<String,String>();
        autoHDFS.populateCredentials(creds);
        LOG.info("Got HDFS credentials", AutoHDFS.getHDFSCredential(creds));

        Subject s = new Subject();
        autoHDFS.populateSubject(s, creds);
        LOG.info("Got a Subject "+ s);

        autoHDFS.renew(creds);
        LOG.info("renewed credentials", AutoHDFS.getHDFSCredential(creds));
    }
}

