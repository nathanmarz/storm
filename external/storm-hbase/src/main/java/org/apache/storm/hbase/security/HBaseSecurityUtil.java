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

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

/**
 * This class provides util methods for storm-hbase connector communicating
 * with secured HBase.
 */
public class HBaseSecurityUtil {
    public static final String STORM_KEYTAB_FILE_KEY = "storm.keytab.file";
    public static final String STORM_USER_NAME_KEY = "storm.kerberos.principal";

    public static UserProvider login(Map conf, Configuration hbaseConfig) throws IOException {
        UserProvider provider = UserProvider.instantiate(hbaseConfig);
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
        return provider;
    }
}
