/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.networktopography;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This is a base class for DNS to Switch mappings.
 *
 * It is not mandatory to
 * derive {@link DNSToSwitchMapping} implementations from it, but it is strongly
 * recommended, as it makes it easy for the developers to add new methods
 * to this base class that are automatically picked up by all implementations.
 *
 */
public abstract class AbstractDNSToSwitchMapping
        implements DNSToSwitchMapping {

    /**
     * Create an unconfigured instance
     */
    protected AbstractDNSToSwitchMapping() {
    }

    /**
     * Predicate that indicates that the switch mapping is known to be
     * single-switch. The base class returns false: it assumes all mappings are
     * multi-rack. Subclasses may override this with methods that are more aware
     * of their topologies.
     *
     *
     *
     * @return true if the mapping thinks that it is on a single switch
     */
    public boolean isSingleSwitch() {
        return false;
    }

    /**
     * Get a copy of the map (for diagnostics)
     * @return a clone of the map or null for none known
     */
    public Map<String, String> getSwitchMap() {
        return null;
    }

    /**
     * Generate a string listing the switch mapping implementation,
     * the mapping for every known node and the number of nodes and
     * unique switches known about -each entry to a separate line.
     *
     * @return a string that can be presented to the ops team or used in
     * debug messages.
     */
    public String dumpTopology() {
        Map<String, String> rack = getSwitchMap();
        StringBuilder builder = new StringBuilder();
        builder.append("Mapping: ").append(toString()).append("\n");
        if (rack != null) {
            builder.append("Map:\n");
            Set<String> switches = new HashSet<>();
            for (Map.Entry<String, String> entry : rack.entrySet()) {
                builder.append("  ")
                        .append(entry.getKey())
                        .append(" -> ")
                        .append(entry.getValue())
                        .append("\n");
                switches.add(entry.getValue());
            }
            builder.append("Nodes: ").append(rack.size()).append("\n");
            builder.append("Switches: ").append(switches.size()).append("\n");
        } else {
            builder.append("No topology information");
        }
        return builder.toString();
    }

}
