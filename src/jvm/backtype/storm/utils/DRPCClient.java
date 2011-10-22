package backtype.storm.utils;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.DRPCRequest;
import backtype.storm.generated.DistributedRPC;
import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;

//TODO: needs to auto-reconnect
public class DRPCClient implements DistributedRPC.Iface {
    private TTransport conn;
    private DistributedRPC.Client client;
    private String host;
    private int port;

    public DRPCClient(String host, int port) {
        try {
            this.host = host;
            this.port = port;
            conn = new TFramedTransport(new TSocket(host, port));
            client = new DistributedRPC.Client(new TBinaryProtocol(conn));
            conn.open();
        } catch(TException e) {
            throw new RuntimeException(e);
        }
    }
    
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }

    public String execute(String func, String args) throws TException, DRPCExecutionException {
        return client.execute(func, args);
    }

    public void result(String id, String result) throws TException {
        client.result(id, result);
    }

    public DRPCRequest fetchRequest(String func) throws TException {
        return client.fetchRequest(func);
    }    

    public void failRequest(String id) throws TException {
        client.failRequest(id);
    }        
    
    public DistributedRPC.Client getClient() {
        return client;
    }

    public void close() {
        conn.close();
    }
}
