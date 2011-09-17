package backtype.storm.utils;

import backtype.storm.generated.DistributedRPC;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;


public class DRPCClient {
    private TTransport conn;
    private DistributedRPC.Client client;

    public DRPCClient(String host, int port) {
        try {
            conn = new TFramedTransport(new TSocket(host, port));
            client = new DistributedRPC.Client(new TBinaryProtocol(conn));
            conn.open();
        } catch(TException e) {
            throw new RuntimeException(e);
        }
    }

    public String execute(String func, String args) throws TException {
        return client.execute(func, args);
    }

    public void result(String id, String result) throws TException {
        client.result(id, result);
    }

    public DistributedRPC.Client getClient() {
        return client;
    }

    public void close() {
        conn.close();
    }
}
