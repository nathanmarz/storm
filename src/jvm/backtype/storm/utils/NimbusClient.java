package backtype.storm.utils;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import java.util.Map;
import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;


public class NimbusClient {
    public static NimbusClient getConfiguredClient(Map conf) {
        String nimbusHost = (String) conf.get(Config.NIMBUS_HOST);
        int nimbusPort = Utils.getInt(conf.get(Config.NIMBUS_THRIFT_PORT));
        return new NimbusClient(nimbusHost, nimbusPort);
    }

    private TTransport conn;
    private Nimbus.Client client;

    public NimbusClient(String host) {
        this(host, 6627);
    }

    public NimbusClient(String host, int port) {
        try {
            if(host==null) {
                throw new IllegalArgumentException("Nimbus host is not set");
            }
            conn = new TFramedTransport(new TSocket(host, port));
            client = new Nimbus.Client(new TBinaryProtocol(conn));
            conn.open();
        } catch(TException e) {
            throw new RuntimeException(e);
        }
    }

    public Nimbus.Client getClient() {
        return client;
    }

    public void close() {
        conn.close();
    }
}
