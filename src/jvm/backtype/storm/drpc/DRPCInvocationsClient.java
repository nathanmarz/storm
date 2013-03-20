package backtype.storm.drpc;

import java.util.Map;

import backtype.storm.generated.DRPCRequest;
import backtype.storm.generated.DistributedRPCInvocations;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.security.auth.ThriftClient;
import org.apache.thrift7.TException;
import org.apache.thrift7.transport.TTransportException;

public class DRPCInvocationsClient extends ThriftClient implements DistributedRPCInvocations.Iface {
    private DistributedRPCInvocations.Client client;
    private String host;
    private int port;    

    public DRPCInvocationsClient(Map conf, String host, int port) throws TTransportException {
        super(conf, host, port, null);
        this.host = host;
        this.port = port;
        client = new DistributedRPCInvocations.Client(_protocol);
    }
        
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }       

    public void result(String id, String result) throws TException, AuthorizationException {
        try {
            client.result(id, result);
        } catch(TException e) {
            client = null;
            throw e;
        }
    }

    public DRPCRequest fetchRequest(String func) throws TException, AuthorizationException {
        try {
            return client.fetchRequest(func);
        } catch(TException e) {
            client = null;
            throw e;
        }
    }    

    public void failRequest(String id) throws TException, AuthorizationException {
        try {
            client.failRequest(id);
        } catch(TException e) {
            client = null;
            throw e;
        }
    }

    public DistributedRPCInvocations.Client getClient() {
        return client;
    }
}
