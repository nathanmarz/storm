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
import java.util.Map;

import javax.security.auth.login.Configuration;

import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Interface for Thrift Transport plugin
 */
public interface ITransportPlugin {
    /**
     * Invoked once immediately after construction
     * @param type the type of connection this will process.
     * @param storm_conf Storm configuration 
     * @param login_conf login configuration
     */
    void prepare(ThriftConnectionType type, Map storm_conf, Configuration login_conf);
    
    /**
     * Create a server associated with a given port, service handler, and purpose
     * @param processor service handler
     * @return server
     */
    public TServer getServer(TProcessor processor) throws IOException, TTransportException;

    /**
     * Connect to the specified server via framed transport 
     * @param transport The underlying Thrift transport.
     * @param serverHost server host
     * @param asUser the user as which the connection should be established, and all the subsequent actions should be executed.
     *               Only applicable when using secure storm cluster. A null/blank value here will just indicate to use the logged in user.
     */
    public TTransport connect(TTransport transport, String serverHost, String asUser) throws IOException, TTransportException;
}
