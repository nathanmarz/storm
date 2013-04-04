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

    public DRPCInvocationsClient(Map conf, String host, int port) throws TTransportException {
        super(conf, host, port, null);
        client = null;
        if (_protocol != null)
            client = new DistributedRPCInvocations.Client(_protocol);
    }
        
    public String getHost() {
        return _host;
    }
    
    public int getPort() {
        return _port;
    }       

    public void result(String id, String result) throws TException, AuthorizationException {
        try {
            if (client == null) {
                connect();
                if (_protocol != null)
                    client = new DistributedRPCInvocations.Client(_protocol);
            }
            client.result(id, result);
        } catch(TException e) {
            client = null;
            throw e;
        }
    }

    public DRPCRequest fetchRequest(String func) throws TException, AuthorizationException {
        try {
            if (client == null) {
                connect();
                if (_protocol != null)
                    client = new DistributedRPCInvocations.Client(_protocol);
            }
            return client.fetchRequest(func);
        } catch(TException e) {
            client = null;
            throw e;
        }
    }    

    public void failRequest(String id) throws TException, AuthorizationException {
        try {
            if (client == null) {
                connect();
                if (_protocol != null)
                    client = new DistributedRPCInvocations.Client(_protocol);
            }
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
