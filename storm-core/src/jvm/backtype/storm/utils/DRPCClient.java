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
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class DRPCClient implements DistributedRPC.Iface {
    private TTransport conn;
    private DistributedRPC.Client client;
    private String host;
    private int port;
    private Integer timeout;

    public DRPCClient(String host, int port, Integer timeout) {
        try {
            this.host = host;
            this.port = port;
            this.timeout = timeout;
            connect();
        } catch(TException e) {
            throw new RuntimeException(e);
        }
    }
    
    public DRPCClient(String host, int port) {
        this(host, port, null);
    }
    
    private void connect() throws TException {
        TSocket socket = new TSocket(host, port);
        if(timeout!=null) {
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
            if(client==null) connect();
            return client.execute(func, args);
        } catch(TException e) {
            client = null;
            throw e;
        } catch(DRPCExecutionException e) {
            client = null;
            throw e;
        }
    }

    public void close() {
        conn.close();
    }
}
