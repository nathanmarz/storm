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

import org.apache.storm.Config;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.zookeeper.Zookeeper;

import java.util.Map;

public class DevZookeeper {
    public static void main(String[] args) throws Exception {
        Map<String, Object> conf = ConfigUtils.readStormConfig();
        Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);
        String localPath = (String) conf.get(Config.DEV_ZOOKEEPER_PATH);
        Utils.forceDelete(localPath);
        Zookeeper.mkInprocessZookeeper(localPath, Utils.getInt(port));
    }
}
