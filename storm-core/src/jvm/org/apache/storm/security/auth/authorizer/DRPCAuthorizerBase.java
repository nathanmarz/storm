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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DRPCAuthorizerBase implements IAuthorizer {
    public static final Logger LOG = LoggerFactory.getLogger(DRPCAuthorizerBase.class);

    /**
     * A key name for the function requested to be executed by a user.
     */
    public static final String FUNCTION_NAME = "function.name";

    @Override
    public abstract void prepare(Map conf);

    abstract protected boolean permitClientRequest(ReqContext context, String operation, Map params);

    abstract protected boolean permitInvocationRequest(ReqContext context, String operation, Map params);
    
    /**
     * Authorizes request from to the DRPC server.
     * @param context the client request context
     * @param operation the operation requested by the DRPC server
     * @param params a Map with any key-value entries of use to the authorization implementation
     */
    @Override
    public boolean permit(ReqContext context, String operation, Map params) {
        if ("execute".equals(operation)) {
            return permitClientRequest(context, operation, params);
        } else if ("failRequest".equals(operation) || 
                "fetchRequest".equals(operation) || 
                "result".equals(operation)) {
            return permitInvocationRequest(context, operation, params);
        }
        // Deny unsupported operations.
        LOG.warn("Denying unsupported operation \""+operation+"\" from "+
                context.remoteAddress());
        return false;
    }
}
