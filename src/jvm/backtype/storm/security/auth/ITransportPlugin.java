package backtype.storm.security.auth;

import java.io.IOException;

import org.apache.thrift7.TProcessor;
import org.apache.thrift7.server.TServer;
import org.apache.thrift7.transport.TTransport;
import org.apache.thrift7.transport.TTransportException;

/**
 * Interface for Thrift Transport plugin
 * 
 * Each plugin should have a constructor 
 *  Foo(Configuration login_conf)
 */
public interface ITransportPlugin {
    /**
     * Create a server for server to use
     * @param port listening port
     * @param processor service handler
     * @return server to be binded
     */
    public TServer getServer(int port, TProcessor processor) throws IOException, TTransportException;
    
    /**
     * Connect to the specified server via framed transport 
     * @param transport The underlying Thrift transport.
     * @param serverHost server host
     */
    public TTransport connect(TTransport transport, String serverHost) throws IOException, TTransportException;
}
