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
import javax.security.auth.kerberos.KerberosTicket;
import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Automatically get HDFS delegation tokens and push it to user's topology.
 */
public class AutoHDFS implements IAutoCredentials, ICredentialsRenewer {
    private static final Logger LOG = LoggerFactory.getLogger(AutoHDFS.class);
    private static final float TICKET_RENEW_WINDOW = 0.80f;
    public static final String HDFS_CREDENTIALS = "HDFS_CREDENTIALS";
    private Map conf;

    public void prepare(Map conf) {
        this.conf = conf;
    }

    private byte[] getHDFSCredsWithDelegationToken() throws Exception {
        try {
            /**
             * What we want to do is following:
             * FileSystem fs = FileSystem.get(nameNodeURI, configuration, topologySubmitterUser);
             * UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(stormUser, topologySubmitter);
             * Credentials credential= proxyUser.getCredentials();
             * fs.addDelegationToken("stormUser", credential);
             *
             * and then return the credential object as a bytearray.
             */
            final URI nameNodeURI = new URI((String) conf.get(Config.HDFS_NAMENODE_URL));
            final String topologySubmitterUser = (String) conf.get(Config.TOPOLOGY_SUBMITTER_USER);
            final String processOwnerUser = System.getProperty("user.name");

            /**
             * FileSystem fs = FileSystem.get(nameNodeURI, configuration, topologySubmitterUser);
             */
            Class fileSystemClass = Class.forName("org.apache.hadoop.fs.FileSystem");
            Class configurationClass = Class.forName("org.apache.hadoop.conf.Configuration");
            Method getMethod = fileSystemClass.getMethod("get", URI.class, configurationClass, String.class);
            Object fileSystem = getMethod.invoke(null, nameNodeURI, configurationClass.newInstance(), topologySubmitterUser);

            //UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(stormUser, topologySubmitter);
            Class ugiClass = Class.forName("org.apache.hadoop.security.UserGroupInformation");
            Method createProxyUserMethod = ugiClass.getMethod("createProxyUser",String.class, ugiClass);
            Object proxyUser = createProxyUserMethod.invoke(processOwnerUser, topologySubmitterUser);

            //Credentials credential= proxyUser.getCredentials();
            Method getCredentialsMethod = ugiClass.getMethod("getCredentials");
            Object credentials = getCredentialsMethod.invoke(proxyUser);

            //fileSystem.addDelegationToken(renewerUser, credentials);
            Class credentialClass = Class.forName("org.apache.hadoop.security.Credentials");
            Method addDelegationTokensMethod = fileSystemClass.getMethod("addDelegationToken", String.class,
                    credentialClass);
            addDelegationTokensMethod.invoke(fileSystem, processOwnerUser, credentials);

            //credentials.write();
            Method writeMethod  = credentialClass.getMethod("write", DataOutput.class);
            ByteArrayOutputStream bao = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bao);
            writeMethod.invoke(credentials, out);
            out.flush();
            out.close();

            return bao.toByteArray();
        } catch (Exception ex) {
            LOG.warn("Failed to get delegation tokens " , ex);
            throw ex;
        }
    }

    @Override
    public void populateCredentials(Map<String, String> credentials) {
        try {
            credentials.put(HDFS_CREDENTIALS, DatatypeConverter.printBase64Binary(getHDFSCredsWithDelegationToken()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Object getHDFSCredential(Map<String, String> credentials) {
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
                throw new RuntimeException(e);
            }
        }
        return credential;
    }

    @Override
    public void updateSubject(Subject subject, Map<String, String> credentials) {
        getUGI(subject, credentials);
    }

    @Override
    public void populateSubject(Subject subject, Map<String, String> credentials) {
        getUGI(subject, credentials);
    }

    private void getUGI(Subject subject, Map<String, String> credentials) {
        try {
            Object credential = getHDFSCredential(credentials);
            if (credential != null) {
                Class ugiClass = Class.forName("org.apache.hadoop.security.UserGroupInformation");
                Constructor constructor = ugiClass.getConstructor(Subject.class);
                Object ugi = constructor.newInstance(subject);

                Class credentialClass = Class.forName("org.apache.hadoop.security.Credentials");
                Method addCredentialsMethod = ugiClass.getMethod("addCredentials", credentialClass);
                addCredentialsMethod.invoke(credential);
            } else {
                LOG.info("No TGT found in credentials");
            }
        } catch (Exception e) {
            LOG.warn("Failed to initialize and get UserGroupInformation.", e);
        }
    }

    private long getRefreshTime(KerberosTicket tgt) {
        long start = tgt.getStartTime().getTime();
        long end = tgt.getEndTime().getTime();
        return start + (long) ((end - start) * TICKET_RENEW_WINDOW);
    }

    @Override
    public void renew(Map<String,String> credentials) {
        Object credential = getHDFSCredential(credentials);
        /**
         * We are trying to do the following :
         * List<Token> tokens = credential.getAllTokens();
         * for(Token token: tokens) {
         *      token.renew(configuration);
         * }
         */
        if (credential != null) {
            try {
                Class credentialClass = Class.forName("org.apache.hadoop.security.Credentials");
                Class tokenClass = Class.forName("org.apache.hadoop.security.token.Token");
                Class configurationClass = Class.forName("org.apache.hadoop.conf.Configuration");
                Object configuration = configurationClass.newInstance();

                Method renewMethod  = tokenClass.getMethod("renew", configurationClass);
                Method getAllTokensMethod = credentialClass.getMethod("getAllTokens");

                List<?> tokens = (List<?>) getAllTokensMethod.invoke(credential);

                for(Object token : tokens) {
                    renewMethod.invoke(token, configuration);
                }
            } catch(Exception e) {
                LOG.warn("could not renew the credentials.", e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        AutoHDFS at = new AutoHDFS();
        Map conf = new java.util.HashMap();
        conf.put("java.security.auth.login.config", args[0]);
        at.prepare(conf);
        Map<String,String> creds = new java.util.HashMap<String,String>();
        at.populateCredentials(creds);
        Subject s = new Subject();
        at.populateSubject(s, creds);
        LOG.info("Got a Subject "+s);
    }
}
