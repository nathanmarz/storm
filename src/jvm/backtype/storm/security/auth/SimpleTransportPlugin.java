package backtype.storm.security.auth;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import javax.security.auth.login.Configuration;
import org.apache.thrift7.TException;
import org.apache.thrift7.TProcessor;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.protocol.TProtocol;
import org.apache.thrift7.server.THsHaServer;
import org.apache.thrift7.server.TServer;
import org.apache.thrift7.server.TThreadPoolServer;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TMemoryInputTransport;
import org.apache.thrift7.transport.TNonblockingServerSocket;
import org.apache.thrift7.transport.TServerSocket;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;
import org.apache.thrift7.transport.TTransportException;
import org.apache.thrift7.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic transport  for Thrift plugin
 */
public class SimpleTransportPlugin implements ITransportPlugin {
    protected Configuration login_conf;
    private static final Logger LOG = LoggerFactory.getLogger(SimpleTransportPlugin.class);

    /**
     * constructor
     */
    public SimpleTransportPlugin(Configuration login_conf) {
        this.login_conf = login_conf;
    }

    /**
     * We will let Thrift to apply default transport factory
     */
    public TServer getServer(int port, TProcessor processor) throws IOException, TTransportException {
        TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(port);
        THsHaServer.Args server_args = new THsHaServer.Args(serverTransport).
                processor(new SimpleWrapProcessor(processor)).
                workerThreads(64).
                protocolFactory(new TBinaryProtocol.Factory());            
        
        //construct THsHaServer
        return new THsHaServer(server_args);
    }

    /**
     * Connect to the specified server via framed transport 
     * @param transport The underlying Thrift transport.
     */
    public TTransport connect(TTransport transport, String serverHost) throws TTransportException {
        //create a framed transport
        TTransport conn = new TFramedTransport(transport);

        //connect
        conn.open();
        LOG.debug("Simple client transport has been established");

        return conn;
    }

    /**                                                                                                                                                                             
     * Processor that populate simple transport info into ReqContext, and then invoke a service handler                                                                              
     */
    private class SimpleWrapProcessor implements TProcessor {
        final TProcessor wrapped;

        SimpleWrapProcessor(TProcessor wrapped) {
            this.wrapped = wrapped;
        }

        public boolean process(final TProtocol inProt, final TProtocol outProt) throws TException {
            //populating request context 
            ReqContext req_context = ReqContext.context();

            TTransport trans = inProt.getTransport();
            if (trans instanceof TMemoryInputTransport) {
                try {
                    req_context.setRemoteAddress(InetAddress.getLocalHost());
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }                                
            } else if (trans instanceof TSocket) {
                TSocket tsocket = (TSocket)trans;
                //remote address
                Socket socket = tsocket.getSocket();
                req_context.setRemoteAddress(socket.getInetAddress());                
            } 

            //anonymous user
            req_context.setSubject(null);
            
            //invoke service handler
            try {
                return wrapped.process(inProt, outProt);
            } catch (RuntimeException ex) {
                LOG.info(ex.getMessage());
                return false;
            }
        }
    } 


}
