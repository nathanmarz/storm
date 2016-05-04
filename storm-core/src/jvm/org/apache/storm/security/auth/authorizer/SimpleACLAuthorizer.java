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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.io.IOException;

import org.apache.storm.Config;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.IPrincipalToLocal;
import org.apache.storm.security.auth.IGroupMappingServiceProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An authorization implementation that simply checks if a user is allowed to perform specific
 * operations.
 */
public class SimpleACLAuthorizer implements IAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleACLAuthorizer.class);

    protected Set<String> _userCommands = new HashSet<>(Arrays.asList("submitTopology", "fileUpload", "getNimbusConf", "getClusterInfo"));
    protected Set<String> _supervisorCommands = new HashSet<>(Arrays.asList("fileDownload"));
    protected Set<String> _topoCommands = new HashSet<>(Arrays.asList(
            "killTopology",
            "rebalance",
            "activate",
            "deactivate",
            "getTopologyConf",
            "getTopology",
            "getUserTopology",
            "getTopologyInfo",
            "getTopologyPageInfo",
            "getComponentPageInfo",
            "uploadNewCredentials",
            "setLogConfig",
            "setWorkerProfiler",
            "getWorkerProfileActionExpiry",
            "getComponentPendingProfileActions",
            "startProfiling",
            "stopProfiling",
            "dumpProfile",
            "dumpJstack",
            "dumpHeap",
            "getLogConfig"));

    protected Set<String> _admins;
    protected Set<String> _supervisors;
    protected Set<String> _nimbusUsers;
    protected Set<String> _nimbusGroups;
    protected IPrincipalToLocal _ptol;
    protected IGroupMappingServiceProvider _groupMappingProvider;
    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration
     */
    @Override
    public void prepare(Map conf) {
        _admins = new HashSet<>();
        _supervisors = new HashSet<>();
        _nimbusUsers = new HashSet<>();
        _nimbusGroups = new HashSet<>();

        if (conf.containsKey(Config.NIMBUS_ADMINS)) {
            _admins.addAll((Collection<String>)conf.get(Config.NIMBUS_ADMINS));
        }
        if (conf.containsKey(Config.NIMBUS_SUPERVISOR_USERS)) {
            _supervisors.addAll((Collection<String>)conf.get(Config.NIMBUS_SUPERVISOR_USERS));
        }
        if (conf.containsKey(Config.NIMBUS_USERS)) {
            _nimbusUsers.addAll((Collection<String>)conf.get(Config.NIMBUS_USERS));
        }

        if (conf.containsKey(Config.NIMBUS_GROUPS)) {
            _nimbusGroups.addAll((Collection<String>)conf.get(Config.NIMBUS_GROUPS));
        }

        _ptol = AuthUtils.GetPrincipalToLocalPlugin(conf);
        _groupMappingProvider = AuthUtils.GetGroupMappingServiceProviderPlugin(conf);
    }

    /**
     * permit() method is invoked for each incoming Thrift request
     * @param context request context includes info about
     * @param operation operation name
     * @param topology_conf configuration of targeted topology
     * @return true if the request is authorized, false if reject
     */
    @Override
    public boolean permit(ReqContext context, String operation, Map topology_conf) {
        String principal = context.principal().getName();
        String user = _ptol.toLocal(context.principal());
        Set<String> userGroups = new HashSet<>();

        if (_groupMappingProvider != null) {
            try {
                userGroups = _groupMappingProvider.getGroups(user);
            } catch(IOException e) {
                LOG.warn("Error while trying to fetch user groups",e);
            }
        }

        if (_admins.contains(principal) || _admins.contains(user)) {
            return true;
        }

        if (_supervisors.contains(principal) || _supervisors.contains(user)) {
            return _supervisorCommands.contains(operation);
        }

        if (_userCommands.contains(operation)) {
            return _nimbusUsers.size() == 0 || _nimbusUsers.contains(user) || checkUserGroupAllowed(userGroups, _nimbusGroups);
        }

        if (_topoCommands.contains(operation)) {
            Set topoUsers = new HashSet<String>();
            if (topology_conf.containsKey(Config.TOPOLOGY_USERS)) {
                topoUsers.addAll((Collection<String>)topology_conf.get(Config.TOPOLOGY_USERS));
            }

            if (topoUsers.contains(principal) || topoUsers.contains(user)) {
                return true;
            }

            Set<String> topoGroups = new HashSet<>();
            if (topology_conf.containsKey(Config.TOPOLOGY_GROUPS) && topology_conf.get(Config.TOPOLOGY_GROUPS) != null) {
                topoGroups.addAll((Collection<String>)topology_conf.get(Config.TOPOLOGY_GROUPS));
            }

            if (checkUserGroupAllowed(userGroups, topoGroups)) return true;
        }
        return false;
    }

    private Boolean checkUserGroupAllowed(Set<String> userGroups, Set<String> configuredGroups) {
        if(userGroups.size() > 0 && configuredGroups.size() > 0) {
            for (String tgroup : configuredGroups) {
                if(userGroups.contains(tgroup))
                    return true;
            }
        }
        return false;
    }
}
