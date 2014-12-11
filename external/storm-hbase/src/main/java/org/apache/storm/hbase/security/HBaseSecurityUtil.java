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
package org.apache.storm.hbase.security;

import static backtype.storm.Config.TOPOLOGY_AUTO_CREDENTIALS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.IOException;
import java.net.InetAddress;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class provides util methods for storm-hbase connector communicating
 * with secured HBase.
 */
public class HBaseSecurityUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseSecurityUtil.class);

    public static final String STORM_KEYTAB_FILE_KEY = "storm.keytab.file";
    public static final String STORM_USER_NAME_KEY = "storm.kerberos.principal";

    public static UserProvider login(Map conf, Configuration hbaseConfig) throws IOException {
        AccessControlContext context = AccessController.getContext();
        Subject subject = Subject.getSubject(context);

        if (subject != null) {
            Set<Credentials> privateCredentials = subject.getPrivateCredentials(Credentials.class);
            if (privateCredentials != null) {
                for (Credentials cred : privateCredentials) {
                    Collection<Token<? extends TokenIdentifier>> allTokens = cred.getAllTokens();
                    if (allTokens != null) {
                        for (Token<? extends TokenIdentifier> token : allTokens) {
                            UserGroupInformation.getCurrentUser().addToken(token);
                            LOG.info("Added Hbase delegation tokens to UGI.");
                        }
                    }
                }
            }
        }

        //Allowing keytab based login for backward compatibility.
        UserProvider provider = UserProvider.instantiate(hbaseConfig);
        if (conf.get(TOPOLOGY_AUTO_CREDENTIALS) == null ||
                !(((List) conf.get(TOPOLOGY_AUTO_CREDENTIALS)).contains(AutoHBase.class.getName()))) {
            LOG.info("Logging in using keytab as AutoHBase is not specified for " + TOPOLOGY_AUTO_CREDENTIALS);
            if (UserGroupInformation.isSecurityEnabled()) {
                String keytab = (String) conf.get(STORM_KEYTAB_FILE_KEY);
                if (keytab != null) {
                    hbaseConfig.set(STORM_KEYTAB_FILE_KEY, keytab);
                }
                String userName = (String) conf.get(STORM_USER_NAME_KEY);
                if (userName != null) {
                    hbaseConfig.set(STORM_USER_NAME_KEY, userName);
                }
                provider.login(STORM_KEYTAB_FILE_KEY, STORM_USER_NAME_KEY,
                        InetAddress.getLocalHost().getCanonicalHostName());
            }
        }
        return provider;
    }
}
