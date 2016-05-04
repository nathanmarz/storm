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
package org.apache.storm.messaging;

import java.util.Map;

/**
 * This interface needs to be implemented for messaging plugin. 
 * 
 * Messaging plugin is specified via Storm config parameter, storm.messaging.transport.
 * 
 * A messaging plugin should have a default constructor and implements IContext interface.
 * Upon construction, we will invoke IContext::prepare(storm_conf) to enable context to be configured
 * according to storm configuration. 
 */
public interface IContext {
    /**
     * This method is invoked at the startup of messaging plugin
     * @param storm_conf storm configuration
     */
    public void prepare(Map storm_conf);
    
    /**
     * This method is invoked when a worker is unload a messaging plugin
     */
    public void term();

    /**
     * This method establishes a server side connection 
     * @param storm_id topology ID
     * @param port port #
     * @return server side connection
     */
    public IConnection bind(String storm_id, int port);
    
    /**
     * This method establish a client side connection to a remote server
     * @param storm_id topology ID
     * @param host remote host
     * @param port remote port
     * @return client side connection
     */
    public IConnection connect(String storm_id, String host, int port);
}
