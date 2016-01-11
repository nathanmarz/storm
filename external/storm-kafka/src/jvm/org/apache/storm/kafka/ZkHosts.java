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
package org.apache.storm.kafka;


public class ZkHosts implements BrokerHosts {
    private static final String DEFAULT_ZK_PATH = "/brokers";

    public String brokerZkStr = null;
    public String brokerZkPath = null; // e.g., /kafka/brokers
    public int refreshFreqSecs = 60;

    public ZkHosts(String brokerZkStr, String brokerZkPath) {
        this.brokerZkStr = brokerZkStr;
        this.brokerZkPath = brokerZkPath;
    }

    public ZkHosts(String brokerZkStr) {
        this(brokerZkStr, DEFAULT_ZK_PATH);
    }
}
