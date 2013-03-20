package backtype.storm.security.auth;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.security.auth.login.Configuration;

import org.apache.thrift7.TProcessor;
import org.apache.thrift7.server.TServer;
import org.apache.thrift7.transport.TTransport;
import org.apache.thrift7.transport.TTransportException;

/**
 * Interface for Thrift Transport plugin
 */
public interface ITransportPlugin {
    /**
     * Invoked once immediately after construction
     * @param storm_conf Storm configuration 
     * @param login_conf login configuration
     * @param executor_service executor service for server
     */
    void prepare(Map storm_conf, Configuration login_conf, ExecutorService executor_service);
    
    /**
     * Create a server associated with a given port and service handler
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
