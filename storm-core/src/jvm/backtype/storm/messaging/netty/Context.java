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
package backtype.storm.messaging.netty;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;

import java.util.Map;
import java.util.Vector;

public class Context implements IContext {
    @SuppressWarnings("rawtypes")
    private Map storm_conf;
    private volatile Vector<IConnection> connections;
    
    /**
     * initialization per Storm configuration 
     */
    @SuppressWarnings("rawtypes")
    public void prepare(Map storm_conf) {
       this.storm_conf = storm_conf;
       connections = new Vector<IConnection>(); 
    }

    /**
     * establish a server with a binding port
     */
    public IConnection bind(String storm_id, int port) {
        IConnection server = new Server(storm_conf, port);
        connections.add(server);
        return server;
    }

    /**
     * establish a connection to a remote server
     */
    public IConnection connect(String storm_id, String host, int port) {        
        IConnection client =  new Client(storm_conf, host, port);
        connections.add(client);
        return client;
    }

    /**
     * terminate this context
     */
    public void term() {
        for (IConnection conn : connections) {
            conn.close();
        }
        connections = null;
    }
}
