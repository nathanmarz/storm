package backtype.storm.utils;

import backtype.storm.Config;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.DistributedRPC;
import backtype.storm.generated.AuthorizationException;
import org.apache.thrift7.TException;
import org.apache.thrift7.transport.TTransport;
import backtype.storm.security.auth.ThriftClient;
import org.apache.thrift7.transport.TTransportException;

import java.util.Map;

public class DRPCClient extends ThriftClient implements DistributedRPC.Iface {
    private TTransport conn;
    private DistributedRPC.Client client;
    private String host;
    private int port;
    private Integer timeout;

    public DRPCClient(Map conf, String host, int port) throws TTransportException {
        this(conf, host, port, null);
    }

    public DRPCClient(Map conf, String host, int port, Integer timeout) throws TTransportException {
        super(conf, host, port, timeout);
        this.host = host;
        this.port = port;
        this.client = new DistributedRPC.Client(_protocol);
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
