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
package backtype.storm.utils;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.DistributedRPC;
import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;

public class DRPCClient implements DistributedRPC.Iface {
    protected TTransport conn;
    protected DistributedRPC.Client client;
    protected String host;
    protected int port;
    protected Integer timeout;

    public DRPCClient(String host, int port, Integer timeout, boolean connectImmediately) {
        try {
            this.host = host;
            this.port = port;
            this.timeout = timeout;
            if (connectImmediately)
                connect();
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public DRPCClient(String host, int port, Integer timeout) {
        this(host, port, timeout, true);
    }

    public DRPCClient(String host, int port) {
        this(host, port, null);
    }

    protected void ensureConnected() throws TException {
        if (!isConnected())
            connect();
    }

    protected boolean isConnected() {
        return conn != null && conn.isOpen();
    }

    protected void connect() throws TException {
        TSocket socket = new TSocket(host, port);
        if (timeout != null) {
            socket.setTimeout(timeout);
        }
        conn = new TFramedTransport(socket);
        client = new DistributedRPC.Client(new TBinaryProtocol(conn));
        conn.open();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String execute(String func, String args) throws TException, DRPCExecutionException {
        try {
            return doExecute(func, args);
        } catch (TException e) {
            close();
            throw e;
        } catch (DRPCExecutionException e) {
            close();
            throw e;
        }
    }

    protected String doExecute(String func, String args) throws TException, DRPCExecutionException {
        ensureConnected();
        return client.execute(func, args);
    }

    public void close() {
        if (conn != null) {
            conn.close();
            conn = null;
        }
        client = null;
    }
}
