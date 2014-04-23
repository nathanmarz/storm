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

import java.io.IOException;
import java.net.Socket;
import java.security.Principal;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.sasl.SaslServer;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for SASL authentication plugin.
 */
public abstract class SaslTransportPlugin implements ITransportPlugin {
    protected Configuration login_conf;
    private static final Logger LOG = LoggerFactory.getLogger(SaslTransportPlugin.class);

    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration 
     * @param login_conf login configuration
     */
    public void prepare(Map storm_conf, Configuration login_conf) {        
        this.login_conf = login_conf;
    }

    public TServer getServer(int port, TProcessor processor) throws IOException, TTransportException {
        TTransportFactory serverTransportFactory = getServerTransportFactory();

        //define THsHaServer args 
        //original: THsHaServer + TNonblockingServerSocket
        //option: TThreadPoolServer + TServerSocket
        TServerSocket serverTransport = new TServerSocket(port);
        TThreadPoolServer.Args server_args = new TThreadPoolServer.Args(serverTransport).
                processor(new TUGIWrapProcessor(processor)).
                minWorkerThreads(64).
                maxWorkerThreads(64).
                protocolFactory(new TBinaryProtocol.Factory());            
        if (serverTransportFactory != null) 
            server_args.transportFactory(serverTransportFactory);

        //construct THsHaServer
        return new TThreadPoolServer(server_args);
    }

    /**
     * All subclass must implement this method
     * @return
     * @throws IOException
     */
    protected abstract TTransportFactory getServerTransportFactory() throws IOException;


    /**                                                                                                                                                                             
     * Processor that pulls the SaslServer object out of the transport, and                                                                                                         
     * assumes the remote user's UGI before calling through to the original                                                                                                         
     * processor.                                                                                                                                                                   
     *                                                                                                                                                                              
     * This is used on the server side to set the UGI for each specific call.                                                                                                       
     */
    private class TUGIWrapProcessor implements TProcessor {
        final TProcessor wrapped;

        TUGIWrapProcessor(TProcessor wrapped) {
            this.wrapped = wrapped;
        }

        public boolean process(final TProtocol inProt, final TProtocol outProt) throws TException {
            //populating request context 
            ReqContext req_context = ReqContext.context();

            TTransport trans = inProt.getTransport();
            //Sasl transport
            TSaslServerTransport saslTrans = (TSaslServerTransport)trans;

            //remote address
            TSocket tsocket = (TSocket)saslTrans.getUnderlyingTransport();
            Socket socket = tsocket.getSocket();
            req_context.setRemoteAddress(socket.getInetAddress());

            //remote subject 
            SaslServer saslServer = saslTrans.getSaslServer();
            String authId = saslServer.getAuthorizationID();
            Subject remoteUser = new Subject();
            remoteUser.getPrincipals().add(new User(authId));
            req_context.setSubject(remoteUser);
            
            //invoke service handler
            return wrapped.process(inProt, outProt);
        }
    }

    public static class User implements Principal {
        private final String name;

        public User(String name) {
            this.name =  name;
        }

        /**                                                                                                                                                                                
         * Get the full name of the user.                                                                                                                                                  
         */
        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            } else {
                return (name.equals(((User) o).name));
            }
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
