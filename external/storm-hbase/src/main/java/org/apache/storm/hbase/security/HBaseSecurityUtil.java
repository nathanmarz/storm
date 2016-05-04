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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import static org.apache.storm.Config.TOPOLOGY_AUTO_CREDENTIALS;

/**
 * This class provides util methods for storm-hbase connector communicating
 * with secured HBase.
 */
public class HBaseSecurityUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseSecurityUtil.class);

    public static final String STORM_KEYTAB_FILE_KEY = "storm.keytab.file";
    public static final String STORM_USER_NAME_KEY = "storm.kerberos.principal";
    private static  UserProvider legacyProvider = null;

    public static UserProvider login(Map conf, Configuration hbaseConfig) throws IOException {
        //Allowing keytab based login for backward compatibility.
        if (UserGroupInformation.isSecurityEnabled() && (conf.get(TOPOLOGY_AUTO_CREDENTIALS) == null ||
                !(((List) conf.get(TOPOLOGY_AUTO_CREDENTIALS)).contains(AutoHBase.class.getName())))) {
            LOG.info("Logging in using keytab as AutoHBase is not specified for " + TOPOLOGY_AUTO_CREDENTIALS);
            //insure that if keytab is used only one login per process executed
            if(legacyProvider == null) {
                synchronized (HBaseSecurityUtil.class) {
                    if(legacyProvider == null) {
                        legacyProvider = UserProvider.instantiate(hbaseConfig);
                        String keytab = (String) conf.get(STORM_KEYTAB_FILE_KEY);
                        if (keytab != null) {
                            hbaseConfig.set(STORM_KEYTAB_FILE_KEY, keytab);
                        }
                        String userName = (String) conf.get(STORM_USER_NAME_KEY);
                        if (userName != null) {
                            hbaseConfig.set(STORM_USER_NAME_KEY, userName);
                        }
                        legacyProvider.login(STORM_KEYTAB_FILE_KEY, STORM_USER_NAME_KEY,
                                InetAddress.getLocalHost().getCanonicalHostName());
                    }
                }
            }
            return legacyProvider;
        } else {
            return UserProvider.instantiate(hbaseConfig);
        }
    }
}
