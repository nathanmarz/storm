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

import org.apache.storm.generated.Nimbus;
import org.apache.storm.utils.NimbusClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Deactivate {
    private static final Logger LOG = LoggerFactory.getLogger(Deactivate.class);

    public static void main(String [] args) throws Exception {
        final String name = args[0];

        NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
          @Override
          public void run(Nimbus.Client nimbus) throws Exception {
            nimbus.deactivate(name);
            LOG.info("Deactivated topology: {}", name);
          }
        });
    }
}
