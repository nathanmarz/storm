package backtype.storm.utils;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.DistributedRPC;
import backtype.storm.generated.AuthorizationException;

import org.apache.thrift7.TException;
import org.apache.thrift7.transport.TTransport;
import backtype.storm.security.auth.ThriftClient;
import org.apache.thrift7.transport.TTransportException;

import java.util.Map;

public class DRPCClient extends ThriftClient implements DistributedRPC.Iface {
    private DistributedRPC.Client client;

    public DRPCClient(Map conf, String host, int port) throws TTransportException {
        this(conf, host, port, null);
    }

    public DRPCClient(Map conf, String host, int port, Integer timeout) throws TTransportException {
        super(conf, host, port, timeout);
        this.client = new DistributedRPC.Client(_protocol);
    }
        
    public String getHost() {
        return _host;
    }
    
    public int getPort() {
        return _port;
    }   
    
    public String execute(String func, String args) throws TException, DRPCExecutionException, AuthorizationException {
        try {
            if(client==null)  {
                connect();
                if (_protocol != null)
                    client = new DistributedRPC.Client(_protocol);
            }
            return client.execute(func, args);
        } catch(TException e) {
            client = null;
            throw e;
        } catch(DRPCExecutionException e) {
            client = null;
            throw e;
        } catch(AuthorizationException e) {
            client = null;
            throw e;
        }
    }

    public DistributedRPC.Client getClient() {
        return client;
    }
}
