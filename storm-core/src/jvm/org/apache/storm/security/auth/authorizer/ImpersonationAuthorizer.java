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

import org.apache.storm.Config;
import org.apache.storm.security.auth.*;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;


public class ImpersonationAuthorizer implements IAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(ImpersonationAuthorizer.class);
    protected static final String WILD_CARD = "*";

    protected Map<String, ImpersonationACL> userImpersonationACL;
    protected IPrincipalToLocal _ptol;
    protected IGroupMappingServiceProvider _groupMappingProvider;

    @Override
    public void prepare(Map conf) {
        userImpersonationACL = new HashMap<>();

        Map<String, Map<String, List<String>>> userToHostAndGroup = (Map<String, Map<String, List<String>>>) conf.get(Config.NIMBUS_IMPERSONATION_ACL);

        if (userToHostAndGroup != null) {
            for (Map.Entry<String, Map<String, List<String>>> entry : userToHostAndGroup.entrySet()) {
                String user = entry.getKey();
                Set<String> groups = ImmutableSet.copyOf(entry.getValue().get("groups"));
                Set<String> hosts = ImmutableSet.copyOf(entry.getValue().get("hosts"));
                userImpersonationACL.put(user, new ImpersonationACL(user, groups, hosts));
            }
        }

        _ptol = AuthUtils.GetPrincipalToLocalPlugin(conf);
        _groupMappingProvider = AuthUtils.GetGroupMappingServiceProviderPlugin(conf);
    }

    @Override
    public boolean permit(ReqContext context, String operation, Map topology_conf) {
        if (!context.isImpersonating()) {
            LOG.debug("Not an impersonation attempt.");
            return true;
        }

        String impersonatingPrincipal = context.realPrincipal().getName();
        String impersonatingUser = _ptol.toLocal(context.realPrincipal());
        String userBeingImpersonated = _ptol.toLocal(context.principal());
        InetAddress remoteAddress = context.remoteAddress();

        LOG.info("user = {}, principal = {} is attempting to impersonate user = {} for operation = {} from host = {}",
                impersonatingUser, impersonatingPrincipal, userBeingImpersonated, operation, remoteAddress);

        /**
         * no config is present for impersonating principal or user, do not permit impersonation.
         */
        if (!userImpersonationACL.containsKey(impersonatingPrincipal) && !userImpersonationACL.containsKey(impersonatingUser)) {
            LOG.info("user = {}, principal = {} is trying to impersonate user {}, but config {} does not have entry for impersonating user or principal." +
                    "Please see SECURITY.MD to learn how to configure users for impersonation."
                    , impersonatingUser, impersonatingPrincipal, userBeingImpersonated, Config.NIMBUS_IMPERSONATION_ACL);
            return false;
        }

        ImpersonationACL principalACL = userImpersonationACL.get(impersonatingPrincipal);
        ImpersonationACL userACL = userImpersonationACL.get(impersonatingUser);

        Set<String> authorizedHosts = new HashSet<>();
        Set<String> authorizedGroups = new HashSet<>();

        if (principalACL != null) {
            authorizedHosts.addAll(principalACL.authorizedHosts);
            authorizedGroups.addAll(principalACL.authorizedGroups);
        }

        if (userACL != null) {
            authorizedHosts.addAll(userACL.authorizedHosts);
            authorizedGroups.addAll(userACL.authorizedGroups);
        }

        LOG.debug("user = {}, principal = {} is allowed to impersonate groups = {} from hosts = {} ",
                impersonatingUser, impersonatingPrincipal, authorizedGroups, authorizedHosts);

        if (!isAllowedToImpersonateFromHost(authorizedHosts, remoteAddress)) {
            LOG.info("user = {}, principal = {} is not allowed to impersonate from host {} ",
                    impersonatingUser, impersonatingPrincipal, remoteAddress);
            return false;
        }

        if (!isAllowedToImpersonateUser(authorizedGroups, userBeingImpersonated)) {
            LOG.info("user = {}, principal = {} is not allowed to impersonate any group that user {} is part of.",
                    impersonatingUser, impersonatingPrincipal, userBeingImpersonated);
            return false;
        }

        LOG.info("Allowing impersonation of user {} by user {}", userBeingImpersonated, impersonatingUser);
        return true;
    }

    private boolean isAllowedToImpersonateFromHost(Set<String> authorizedHosts, InetAddress remoteAddress) {
        return authorizedHosts.contains(WILD_CARD) ||
                authorizedHosts.contains(remoteAddress.getCanonicalHostName()) ||
                authorizedHosts.contains(remoteAddress.getHostName()) ||
                authorizedHosts.contains(remoteAddress.getHostAddress());
    }

    private boolean isAllowedToImpersonateUser(Set<String> authorizedGroups, String userBeingImpersonated) {
        if(authorizedGroups.contains(WILD_CARD)) {
            return true;
        }

        Set<String> groups;
        try {
            groups = _groupMappingProvider.getGroups(userBeingImpersonated);
        } catch (IOException e) {
            throw new RuntimeException("failed to get groups for user " + userBeingImpersonated);
        }

        if (groups == null || groups.isEmpty()) {
            return false;
        }

        for (String group : groups) {
            if (authorizedGroups.contains(group)) {
                return true;
            }
        }

        return false;
    }

    protected static class ImpersonationACL {
        public String impersonatingUser;
        //Groups this user is authorized to impersonate.
        public Set<String> authorizedGroups;
        //Hosts this user is authorized to impersonate from.
        public Set<String> authorizedHosts;

        private ImpersonationACL(String impersonatingUser, Set<String> authorizedGroups, Set<String> authorizedHosts) {
            this.impersonatingUser = impersonatingUser;
            this.authorizedGroups = authorizedGroups;
            this.authorizedHosts = authorizedHosts;
        }

        @Override
        public String toString() {
            return "ImpersonationACL{" +
                    "impersonatingUser='" + impersonatingUser + '\'' +
                    ", authorizedGroups=" + authorizedGroups +
                    ", authorizedHosts=" + authorizedHosts +
                    '}';
        }
    }
}
