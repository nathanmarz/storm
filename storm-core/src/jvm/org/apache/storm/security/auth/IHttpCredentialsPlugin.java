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
import javax.servlet.http.HttpServletRequest;

/**
 * Interface for handling credentials in an HttpServletRequest
 */
public interface IHttpCredentialsPlugin {
    /**
     * Invoked once immediately after construction
     * @param storm_conf Storm configuration
     */
    void prepare(Map storm_conf);

    /**
     * Gets the user name from the request.
     * @param req the servlet request
     * @return the authenticated user, or null if none is authenticated.
     */
    String getUserName(HttpServletRequest req);

    /**
     * Populates a given context with credentials information from an HTTP
     * request.
     * @param req the servlet request
     * @return the context
     */
    ReqContext populateContext(ReqContext context, HttpServletRequest req);
}
