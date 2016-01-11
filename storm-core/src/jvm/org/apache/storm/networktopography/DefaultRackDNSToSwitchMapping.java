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
package org.apache.storm.networktopography;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements the {@link DNSToSwitchMapping} interface
 *    It returns the DEFAULT_RACK for every host.
 */
public final class DefaultRackDNSToSwitchMapping extends AbstractDNSToSwitchMapping {

    private Map<String, String> mappingCache = new ConcurrentHashMap<>();

    @Override
    public Map<String,String> resolve(List<String> names) {

        Map<String, String> m = new HashMap<>();
        if (names.isEmpty()) {
            //name list is empty, return an empty map
            return m;
        }
        for (String name : names) {
            m.put(name, DEFAULT_RACK);
            mappingCache.put(name, DEFAULT_RACK);
        }
        return m;
    }

    @Override
    public String toString() {
        return "DefaultRackDNSToSwitchMapping (" + mappingCache.size() + " mappings cached)" + dumpTopology();
    }
}
