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

package org.apache.storm.hive.common;

import org.apache.storm.hive.common.HiveWriter;
import org.apache.storm.hive.bolt.mapper.HiveMapper;
import org.apache.hive.hcatalog.streaming.*;

import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.io.File;
import java.io.IOException;

public class HiveUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HiveUtils.class);

    public static HiveEndPoint makeEndPoint(List<String> partitionVals, HiveOptions options) throws ConnectionError {
        if(partitionVals==null) {
            return new HiveEndPoint(options.getMetaStoreURI(), options.getDatabaseName(), options.getTableName(), null);
        }
        return new HiveEndPoint(options.getMetaStoreURI(), options.getDatabaseName(), options.getTableName(), partitionVals);
    }

    public static HiveWriter makeHiveWriter(HiveEndPoint endPoint, ExecutorService callTimeoutPool, UserGroupInformation ugi, HiveOptions options)
        throws HiveWriter.ConnectFailure, InterruptedException {
        return new HiveWriter(endPoint, options.getTxnsPerBatch(), options.getAutoCreatePartitions(),
                              options.getCallTimeOut(), callTimeoutPool, options.getMapper(), ugi);
    }

    public static synchronized UserGroupInformation authenticate(String keytab, String principal)
    throws AuthenticationFailed {
        File kfile = new File(keytab);
        if (!(kfile.isFile() && kfile.canRead())) {
            throw new IllegalArgumentException("The keyTab file: "
                                               + keytab + " is nonexistent or can't read. "
                                               + "Please specify a readable keytab file for Kerberos auth.");
        }
        try {
            principal = SecurityUtil.getServerPrincipal(principal, "");
        } catch (Exception e) {
            throw new AuthenticationFailed("Host lookup error when resolving principal " + principal, e);
        }
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
            return UserGroupInformation.getLoginUser();
        } catch (IOException e) {
            throw new AuthenticationFailed("Login failed for principal " + principal, e);
        }
    }

     public static class AuthenticationFailed extends Exception {
         public AuthenticationFailed(String reason, Exception cause) {
             super("Kerberos Authentication Failed. " + reason, cause);
         }
     }

    public static void logAllHiveEndPoints(Map<HiveEndPoint, HiveWriter> allWriters) {
        for (Map.Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
            LOG.info("cached writers {} ", entry.getValue());
        }
    }
}
