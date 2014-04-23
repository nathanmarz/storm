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
package backtype.storm.security.auth;

import java.util.Map;

/**
 * Nimbus could be configured with an authorization plugin.
 * If not specified, all requests are authorized.
 * 
 * You could specify the authorization plugin via storm parameter. For example:
 *  storm -c nimbus.authorization.class=backtype.storm.security.auth.NoopAuthorizer ...
 *  
 * You could also specify it via storm.yaml:
 *   nimbus.authorization.class: backtype.storm.security.auth.NoopAuthorizer
 */
public interface IAuthorizer {
    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration 
     */
    void prepare(Map storm_conf);
    
    /**
     * permit() method is invoked for each incoming Thrift request.
     * @param context request context includes info about 
     * @param operation operation name
     * @param topology_storm configuration of targeted topology 
     * @return true if the request is authorized, false if reject
     */
    public boolean permit(ReqContext context, String operation, Map topology_conf);
}
