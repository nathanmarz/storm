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
package org.apache.storm.command;

import org.apache.storm.StormSubmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class UploadCredentials {

    private static final Logger LOG = LoggerFactory.getLogger(UploadCredentials.class);

    public static void main(String[] args) throws Exception {
        Map<String, Object> cl = CLI.opt("f", "file", null)
            .arg("topologyName", CLI.FIRST_WINS)
            .arg("rawCredentials", CLI.INTO_LIST)
            .parse(args);

        String credentialFile = (String) cl.get("f");
        List<String> rawCredentials = (List<String>) cl.get("rawCredentials");
        String topologyName = (String) cl.get("topologyName");

        if (null != rawCredentials && ((rawCredentials.size() % 2) != 0)) {
            throw new RuntimeException("Need an even number of arguments to make a map");
        }
        Map credentialsMap = new HashMap<>();
        if (null != credentialFile) {
            Properties credentialProps = new Properties();
            credentialProps.load(new FileReader(credentialFile));
            credentialsMap.putAll(credentialProps);
        }
        if (null != rawCredentials) {
            for (int i = 0; i < rawCredentials.size(); i += 2) {
                credentialsMap.put(rawCredentials.get(i), rawCredentials.get(i + 1));
            }
        }
        StormSubmitter.pushCredentials(topologyName, new HashMap(), credentialsMap);
        LOG.info("Uploaded new creds to topology: {}", topologyName);
    }
}
