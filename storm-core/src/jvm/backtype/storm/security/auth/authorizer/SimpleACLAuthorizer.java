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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.io.IOException;

import backtype.storm.Config;
import backtype.storm.security.auth.IAuthorizer;
import backtype.storm.security.auth.ReqContext;
import backtype.storm.security.auth.AuthUtils;
import backtype.storm.security.auth.IPrincipalToLocal;
import backtype.storm.security.auth.IGroupMappingServiceProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An authorization implementation that simply checks if a user is allowed to perform specific
 * operations.
 */
public class SimpleACLAuthorizer implements IAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleACLAuthorizer.class);

    protected Set<String> _userCommands = new HashSet<String>(Arrays.asList("submitTopology", "fileUpload", "getNimbusConf", "getClusterInfo"));
    protected Set<String> _supervisorCommands = new HashSet<String>(Arrays.asList("fileDownload"));
    protected Set<String> _topoCommands = new HashSet<String>(Arrays.asList("killTopology","rebalance","activate","deactivate","getTopologyConf","getTopology","getUserTopology","getTopologyInfo","uploadNewCredentials"));

    protected Set<String> _admins;
    protected Set<String> _supervisors;
    protected IPrincipalToLocal _ptol;
    protected IGroupMappingServiceProvider _groupMappingProvider;
    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration
     */
    @Override
    public void prepare(Map conf) {
        _admins = new HashSet<String>();
        _supervisors = new HashSet<String>();

        if (conf.containsKey(Config.NIMBUS_ADMINS)) {
            _admins.addAll((Collection<String>)conf.get(Config.NIMBUS_ADMINS));
        }
        if (conf.containsKey(Config.NIMBUS_SUPERVISOR_USERS)) {
            _supervisors.addAll((Collection<String>)conf.get(Config.NIMBUS_SUPERVISOR_USERS));
        }

        _ptol = AuthUtils.GetPrincipalToLocalPlugin(conf);
        _groupMappingProvider = AuthUtils.GetGroupMappingServiceProviderPlugin(conf);
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

        String principal = context.principal().getName();
        String user = _ptol.toLocal(context.principal());
        if (_admins.contains(principal) || _admins.contains(user)) {
            return true;
        }

        if (_supervisors.contains(principal) || _supervisors.contains(user)) {
            return _supervisorCommands.contains(operation);
        }

        if (_userCommands.contains(operation)) {
            return true;
        }

        if (_topoCommands.contains(operation)) {
            Set topoUsers = new HashSet<String>();
            if (topology_conf.containsKey(Config.TOPOLOGY_USERS)) {
                topoUsers.addAll((Collection<String>)topology_conf.get(Config.TOPOLOGY_USERS));
            }

            if (topoUsers.contains(principal) || topoUsers.contains(user)) {
                return true;
            }

            Set<String> topoGroups = new HashSet<String>();
            if (topology_conf.containsKey(Config.TOPOLOGY_GROUPS)) {
                topoGroups.addAll((Collection<String>)topology_conf.get(Config.TOPOLOGY_GROUPS));
            }

            if(_groupMappingProvider != null && topoGroups.size() > 0) {
                try {
                    Set<String> userGroups = _groupMappingProvider.getGroups(user);
                    for (String tgroup : topoGroups) {
                        if(userGroups.contains(tgroup))
                            return true;
                    }
                } catch(IOException e) {
                    LOG.warn("Error while trying to fetch user groups",e);
                }
            }
        }
        return false;
    }
}
