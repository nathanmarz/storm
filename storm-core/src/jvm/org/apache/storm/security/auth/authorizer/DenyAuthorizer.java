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
package org.apache.storm.security.auth.authorizer;

import java.util.Map;

import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;

/**
 * An authorization implementation that denies everything, for testing purposes
 */
public class DenyAuthorizer implements IAuthorizer {

    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration 
     */
    public void prepare(Map conf) {        
    }

    /**
     * permit() method is invoked for each incoming Thrift request
     * @param context request context
     * @param operation operation name
     * @param topology_conf configuration of targeted topology
     * @return true if the request is authorized, false if reject
     */
    public boolean permit(ReqContext context, String operation, Map topology_conf) {
        return false;
    }
}
