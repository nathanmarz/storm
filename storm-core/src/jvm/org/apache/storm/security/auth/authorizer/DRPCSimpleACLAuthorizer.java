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

import java.lang.reflect.Field;
import java.security.Principal;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.IPrincipalToLocal;
import org.apache.storm.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DRPCSimpleACLAuthorizer extends DRPCAuthorizerBase {
    public static final Logger LOG =
        LoggerFactory.getLogger(DRPCSimpleACLAuthorizer.class);

    public static final String CLIENT_USERS_KEY = "client.users";
    public static final String INVOCATION_USER_KEY = "invocation.user";
    public static final String FUNCTION_KEY = "function.name";

    protected String _aclFileName = "";
    protected IPrincipalToLocal _ptol;
    protected boolean _permitWhenMissingFunctionEntry = false;

    protected static class AclFunctionEntry {
        final public Set<String> clientUsers;
        final public String invocationUser;
        public AclFunctionEntry(Collection<String> clientUsers,
                String invocationUser) {
            this.clientUsers = (clientUsers != null) ?
                new HashSet<>(clientUsers) : new HashSet<String>();
            this.invocationUser = invocationUser;
        }
    }

    private volatile Map<String,AclFunctionEntry> _acl = null;
    private volatile long _lastUpdate = 0;

    protected Map<String,AclFunctionEntry> readAclFromConfig() {
        //Thread safety is mostly around _acl.  If _acl needs to be updated it is changed atomically
        //More then one thread may be trying to update it at a time, but that is OK, because the
        //change is atomic
        long now = System.currentTimeMillis();
        if ((now - 5000) > _lastUpdate || _acl == null) {
            Map<String,AclFunctionEntry> acl = new HashMap<>();
            Map conf = Utils.findAndReadConfigFile(_aclFileName);
            if (conf.containsKey(Config.DRPC_AUTHORIZER_ACL)) {
                Map<String,Map<String,?>> confAcl =
                    (Map<String,Map<String,?>>)
                    conf.get(Config.DRPC_AUTHORIZER_ACL);

                for (Map.Entry<String, Map<String, ?>> entry : confAcl.entrySet()) {
                    Map<String,?> val = entry.getValue();
                    Collection<String> clientUsers =
                        val.containsKey(CLIENT_USERS_KEY) ?
                        (Collection<String>) val.get(CLIENT_USERS_KEY) : null;
                    String invocationUser =
                        val.containsKey(INVOCATION_USER_KEY) ?
                        (String) val.get(INVOCATION_USER_KEY) : null;
                    acl.put(entry.getKey(),
                            new AclFunctionEntry(clientUsers, invocationUser));
                }
            } else if (!_permitWhenMissingFunctionEntry) {
                LOG.warn("Requiring explicit ACL entries, but none given. " +
                        "Therefore, all operations will be denied.");
            }
            _acl = acl;
            _lastUpdate = System.currentTimeMillis();
        }
        return _acl;
    }

    @Override
    public void prepare(Map conf) {
        Boolean isStrict = 
                (Boolean) conf.get(Config.DRPC_AUTHORIZER_ACL_STRICT);
        _permitWhenMissingFunctionEntry =
                (isStrict != null && !isStrict);
        _aclFileName = (String) conf.get(Config.DRPC_AUTHORIZER_ACL_FILENAME);
        _ptol = AuthUtils.GetPrincipalToLocalPlugin(conf);
    }

    private String getUserFromContext(ReqContext context) {
        if (context != null) {
            Principal princ = context.principal();
            if (princ != null) {
                return princ.getName();
            }
        }
        return null;
    }

    private String getLocalUserFromContext(ReqContext context) {
        if (context != null) {
            return _ptol.toLocal(context.principal());
        }
        return null;
    }

    protected boolean permitClientOrInvocationRequest(ReqContext context, Map params,
            String fieldName) {
        Map<String,AclFunctionEntry> acl = readAclFromConfig();
        String function = (String) params.get(FUNCTION_KEY);
        if (function != null && ! function.isEmpty()) {
            AclFunctionEntry entry = acl.get(function);
            if (entry == null && _permitWhenMissingFunctionEntry) {
                return true;
            }
            if (entry != null) {
                Object value;
                try {
                    Field field = AclFunctionEntry.class.getDeclaredField(fieldName);
                    value = field.get(entry);
                } catch (Exception ex) {
                    LOG.warn("Caught Exception while accessing ACL", ex);
                    return false;
                }
                String principal = getUserFromContext(context);
                String user = getLocalUserFromContext(context);
                if (value == null) {
                    LOG.warn("Configuration for function '"+function+"' is "+
                            "invalid: it should have both an invocation user "+
                            "and a list of client users defined.");
                } else if (value instanceof Set && 
                        (((Set<String>)value).contains(principal) ||
                        ((Set<String>)value).contains(user))) {
                    return true;
                } else if (value instanceof String && 
                        (value.equals(principal) ||
                         value.equals(user))) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    protected boolean permitClientRequest(ReqContext context, String operation,
            Map params) {
        return permitClientOrInvocationRequest(context, params, "clientUsers");
    }

    @Override
    protected boolean permitInvocationRequest(ReqContext context, String operation,
            Map params) {
        return permitClientOrInvocationRequest(context, params, "invocationUser");
    }
}
