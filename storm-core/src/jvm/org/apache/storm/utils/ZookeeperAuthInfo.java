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
package org.apache.storm.utils;

import org.apache.storm.Config;
import java.io.UnsupportedEncodingException;
import java.util.Map;


public class ZookeeperAuthInfo {
    public String scheme;
    public byte[] payload = null;
    
    public ZookeeperAuthInfo(Map conf) {
        String scheme = (String) conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME);
        String payload = (String) conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);

        if (scheme == null || payload == null) {
            scheme = (String) conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME);
            payload = (String) conf.get(Config.STORM_ZOOKEEPER_AUTH_PAYLOAD);
        }
        if(scheme!=null) {
            this.scheme = scheme;
            if(payload != null) {
                try {
                    this.payload = payload.getBytes("UTF-8");
                } catch (UnsupportedEncodingException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }
    
    public ZookeeperAuthInfo(String scheme, byte[] payload) {
        this.scheme = scheme;
        this.payload = payload;
    }
}
