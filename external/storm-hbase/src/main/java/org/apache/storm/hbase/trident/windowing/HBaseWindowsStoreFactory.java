/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.hbase.trident.windowing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.storm.trident.windowing.WindowsStore;
import org.apache.storm.trident.windowing.WindowsStoreFactory;

import java.util.Map;

/**
 * Factory to create {@link HBaseWindowsStore} instances.
 *
 */
public class HBaseWindowsStoreFactory implements WindowsStoreFactory {
    private final Map<String, Object> config;
    private final String tableName;
    private final byte[] family;
    private final byte[] qualifier;

    public HBaseWindowsStoreFactory(Map<String, Object> config, String tableName, byte[] family, byte[] qualifier) {
        this.config = config;
        this.tableName = tableName;
        this.family = family;
        this.qualifier = qualifier;
    }

    public WindowsStore create(Map stormConf) {
        Configuration configuration = HBaseConfiguration.create();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            if (entry.getValue() != null) {
                configuration.set(entry.getKey(), entry.getValue().toString());
            }
        }
        return new HBaseWindowsStore(stormConf, configuration, tableName, family, qualifier);
    }

}
