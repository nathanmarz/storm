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
package org.apache.storm.drpc;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.generated.DistributedRPCInvocations;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.security.auth.ThriftClient;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DRPCInvocationsClient extends ThriftClient implements DistributedRPCInvocations.Iface {
    public static final Logger LOG = LoggerFactory.getLogger(DRPCInvocationsClient.class);
    private final AtomicReference<DistributedRPCInvocations.Client> client = new AtomicReference<>();
    private String host;
    private int port;

    public DRPCInvocationsClient(Map conf, String host, int port) throws TTransportException {
        super(conf, ThriftConnectionType.DRPC_INVOCATIONS, host, port, null);
        this.host = host;
        this.port = port;
        client.set(new DistributedRPCInvocations.Client(_protocol));
    }
        
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }       

    public void reconnectClient() throws TException {
        if (client.get() == null) {
            reconnect();
            client.set(new DistributedRPCInvocations.Client(_protocol));
        }
    }

    public boolean isConnected() {
        return client.get() != null;
    }

    public void result(String id, String result) throws TException, AuthorizationException {
        DistributedRPCInvocations.Client c = client.get();
        try {
            if (c == null) {
                throw new TException("Client is not connected...");
            }
            c.result(id, result);
        } catch(AuthorizationException aze) {
            throw aze;
        } catch(TException e) {
            client.compareAndSet(c, null);
            throw e;
        }
    }

    public DRPCRequest fetchRequest(String func) throws TException, AuthorizationException {
        DistributedRPCInvocations.Client c = client.get();
        try {
            if (c == null) {
                throw new TException("Client is not connected...");
            }
            return c.fetchRequest(func);
        } catch(AuthorizationException aze) {
            throw aze;
        } catch(TException e) {
            client.compareAndSet(c, null);
            throw e;
        }
    }    

    public void failRequest(String id) throws TException, AuthorizationException {
        DistributedRPCInvocations.Client c = client.get();
        try {
            if (c == null) {
                throw new TException("Client is not connected...");
            }
            c.failRequest(id);
        } catch(AuthorizationException aze) {
            throw aze;
        } catch(TException e) {
            client.compareAndSet(c, null);
            throw e;
        }
    }

    public DistributedRPCInvocations.Client getClient() {
        return client.get();
    }
}
