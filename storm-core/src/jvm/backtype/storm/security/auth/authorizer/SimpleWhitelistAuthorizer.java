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

package backtype.storm.security.auth.authorizer;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;

import backtype.storm.Config;
import backtype.storm.security.auth.IAuthorizer;
import backtype.storm.security.auth.ReqContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An authorization implementation that simply checks a whitelist of users that
 * are allowed to use the cluster.
 */
public class SimpleWhitelistAuthorizer implements IAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleWhitelistAuthorizer.class);
    public static String WHITELIST_USERS_CONF = "storm.auth.simple-white-list.users";
    protected Set<String> users;

    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration 
     */
    @Override
    public void prepare(Map conf) {
        users = new HashSet<String>();
        if (conf.containsKey(WHITELIST_USERS_CONF)) {
            users.addAll((Collection<String>)conf.get(WHITELIST_USERS_CONF));
        }
    }

    /**
     * permit() method is invoked for each incoming Thrift request
     * @param context request context includes info about 
     * @param operation operation name
     * @param topology_storm configuration of targeted topology 
     * @return true if the request is authorized, false if reject
     */
    @Override
    public boolean permit(ReqContext context, String operation, Map topology_conf) {
        LOG.info("[req "+ context.requestID()+ "] Access "
                 + " from: " + (context.remoteAddress() == null? "null" : context.remoteAddress().toString())
                 + (context.principal() == null? "" : (" principal:"+ context.principal()))
                 +" op:"+operation
                 + (topology_conf == null? "" : (" topoology:"+topology_conf.get(Config.TOPOLOGY_NAME))));
        return context.principal() != null ? users.contains(context.principal().getName()) : false;
    }
}
