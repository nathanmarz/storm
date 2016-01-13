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
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.storm.Config;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.ShellUtils;
import org.apache.storm.utils.TimeCacheMap;
import org.apache.storm.utils.ShellUtils.ExitCodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ShellBasedGroupsMapping implements
                                             IGroupMappingServiceProvider {

    public static final Logger LOG = LoggerFactory.getLogger(ShellBasedGroupsMapping.class);
    public TimeCacheMap<String, Set<String>> cachedGroups;

    /**
     * Invoked once immediately after construction
     * @param storm_conf Storm configuration
     */
    @Override
    public void prepare(Map storm_conf) {
        int timeout = Utils.getInt(storm_conf.get(Config.STORM_GROUP_MAPPING_SERVICE_CACHE_DURATION_SECS));
        cachedGroups = new TimeCacheMap<>(timeout);
    }

    /**
     * Returns list of groups for a user
     *
     * @param user get groups for this user
     * @return list of groups for a given user
     */
    @Override
    public Set<String> getGroups(String user) throws IOException {
        if(cachedGroups.containsKey(user)) {
            return cachedGroups.get(user);
        }
        Set<String> groups = getUnixGroups(user);
        if(!groups.isEmpty())
            cachedGroups.put(user,groups);
        return groups;
    }

    /**
     * Get the current user's group list from Unix by running the command 'groups'
     * NOTE. For non-existing user it will return EMPTY list
     * @param user user name
     * @return the groups set that the <code>user</code> belongs to
     * @throws IOException if encounter any error when running the command
     */
    private static Set<String> getUnixGroups(final String user) throws IOException {
        String result;
        try {
            result = ShellUtils.execCommand(ShellUtils.getGroupsForUserCommand(user));
        } catch (ExitCodeException e) {
            // if we didn't get the group - just return empty list;
            LOG.debug("unable to get groups for user " + user + ".ShellUtils command failed with exit code "+ e.getExitCode());
            return new HashSet<>();
        }

        StringTokenizer tokenizer =
            new StringTokenizer(result, ShellUtils.TOKEN_SEPARATOR_REGEX);
        Set<String> groups = new HashSet<>();
        while (tokenizer.hasMoreTokens()) {
            groups.add(tokenizer.nextToken());
        }
        return groups;
    }

}
