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
package org.apache.storm.testing;

import org.apache.storm.networktopography.AbstractDNSToSwitchMapping;
import org.apache.storm.networktopography.DNSToSwitchMapping;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements the {@link DNSToSwitchMapping} interface
 *    It alternates bewteen RACK1 and RACK2 for the hosts.
 */
public final class AlternateRackDNSToSwitchMapping extends AbstractDNSToSwitchMapping {

  private Map<String, String> mappingCache = new ConcurrentHashMap<String, String>();

  @Override
  public Map<String, String> resolve(List<String> names) {
    TreeSet<String> sortedNames = new TreeSet<String>(names);
    Map <String, String> m = new HashMap<String, String>();
    if (names.isEmpty()) {
      //name list is empty, return an empty map
      return m;
    }

    Boolean odd = true;
    for (String name : sortedNames) {
      if (odd) {
        m.put(name, "RACK1");
        mappingCache.put(name, "RACK1");
        odd = false;
      } else {
        m.put(name, "RACK2");
        mappingCache.put(name, "RACK2");
        odd = true;
      }
    }
    return m;
  }

  @Override
  public String toString() {
    return "defaultRackDNSToSwitchMapping (" + mappingCache.size() + " mappings cached)";
  }
}
