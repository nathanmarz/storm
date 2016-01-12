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

import java.util.Map;
import java.security.Principal;

/**
 * Storm can be configured to launch worker processed as a given user.
 * Some transports need to map the Principal to a local user name.
 */
public interface IPrincipalToLocal {
    /**
     * Invoked once immediately after construction
     * @param storm_conf Storm configuration
     */
    void prepare(Map storm_conf);
    
    /**
     * Convert a Principal to a local user name.
     * @param principal the principal to convert
     * @return The local user name.
     */
    public String toLocal(Principal principal);
}
