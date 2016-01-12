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

import java.util.List;
import java.util.Map;

/**
 * An interface that must be implemented to allow pluggable
 * DNS-name/IP-address to RackID resolvers.
 *
 */
public interface DNSToSwitchMapping {
    public final static String DEFAULT_RACK = "/default-rack";

    /**
     * Resolves a list of DNS-names/IP-address and returns back a map of DNS-name->switch information ( network paths).
     * Consider an element in the argument list - x.y.com. The switch information
     * that is returned must be a network path of the form /foo/rack,
     * where / is the root, and 'foo' is the switch where 'rack' is connected.
     * Note the hostname/ip-address is not part of the returned path.
     * The network topology of the cluster would determine the number of
     * components in the network path.
     *
     * If a name cannot be resolved to a rack, the implementation
     * should return {DEFAULT_RACK}. This
     * is what the bundled implementations do, though it is not a formal requirement
     *
     * @param names the list of hosts to resolve (can be empty)
     * @return Map of hosts to resolved network paths.
     * If <i>names</i> is empty, then return empty Map
     */
    public Map<String, String> resolve(List<String> names);
}
