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
package org.apache.storm.utils;

import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DistributedRPC;
import org.apache.storm.generated.AuthorizationException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.storm.security.auth.ThriftClient;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.thrift.transport.TTransportException;

import java.util.Map;

public class DRPCClient extends ThriftClient implements DistributedRPC.Iface {
    private TTransport conn;
    private DistributedRPC.Client client;
    private String host;
    private int port;
    private Integer timeout;

    public DRPCClient(Map conf, String host, int port) throws TTransportException {
        this(conf, host, port, null);
        _retryForever = true;
    }

    public DRPCClient(Map conf, String host, int port, Integer timeout) throws TTransportException {
        super(conf, ThriftConnectionType.DRPC, host, port, timeout, null);
        this.host = host;
        this.port = port;
        this.client = new DistributedRPC.Client(_protocol);
        _retryForever = true;
    }
        
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
    
    public String execute(String func, String args) throws TException, DRPCExecutionException, AuthorizationException {
        return client.execute(func, args);
    }

    public DistributedRPC.Client getClient() {
        return client;
    }
}
