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

package org.apache.storm.security.auth;

import java.io.IOException;
import java.util.Set;
import java.util.Map;

public interface IGroupMappingServiceProvider {

    /**
     * Invoked once immediately after construction
     * @param storm_conf Storm configuration
     */
    void prepare(Map storm_conf);

    /**
     * Get all various group memberships of a given user.
     * Returns EMPTY list in case of non-existing user
     * @param user User's name
     * @return group memberships of user
     * @throws IOException
     */
    public Set<String> getGroups(String user) throws IOException;

}
