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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.Map;

/**
 * This class provides util methods for storm-hdfs connector communicating
 * with secured HDFS.
 */
public class HdfsSecurityUtil {
    public static final String STORM_KEYTAB_FILE_KEY = "hdfs.keytab.file";
    public static final String STORM_USER_NAME_KEY = "hdfs.kerberos.principal";

    public static void login(Map conf, Configuration hdfsConfig) throws IOException {
        if (UserGroupInformation.isSecurityEnabled()) {
            String keytab = (String) conf.get(STORM_KEYTAB_FILE_KEY);
            if (keytab != null) {
              hdfsConfig.set(STORM_KEYTAB_FILE_KEY, keytab);
            }
            String userName = (String) conf.get(STORM_USER_NAME_KEY);
            if (userName != null) {
              hdfsConfig.set(STORM_USER_NAME_KEY, userName);
            }
            SecurityUtil.login(hdfsConfig, STORM_KEYTAB_FILE_KEY, STORM_USER_NAME_KEY);
        }
    }
}
