package backtype.storm.utils;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;

import java.net.InetSocketAddress;
import java.util.Map;

import backtype.storm.nimbus.NimbusLeaderElections;
import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;


public class NimbusClient {
    public static NimbusClient getConfiguredClient(Map conf) {
        NimbusLeaderElections elections = new NimbusLeaderElections();
        elections.init(conf, null);
        InetSocketAddress address = elections.getLeaderAddr();
        String nimbusHost = address.getHostName();
        int nimbusPort = address.getPort();
        return new NimbusClient(nimbusHost, nimbusPort);
    }


    private TTransport conn;
    private Nimbus.Client client;

    public NimbusClient(String host, int port) {
        try {
            if (host == null) {
                throw new IllegalArgumentException("Nimbus host is not set");
            }
            conn = new TFramedTransport(new TSocket(host, port));
            client = new Nimbus.Client(new TBinaryProtocol(conn));
            conn.open();
        } catch (TException e) {
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
