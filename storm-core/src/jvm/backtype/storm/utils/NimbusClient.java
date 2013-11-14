package backtype.storm.utils;

import backtype.storm.Config;
import backtype.storm.security.auth.ThriftClient;
import backtype.storm.generated.Nimbus;
import java.util.Map;
import org.apache.thrift7.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NimbusClient extends ThriftClient {
    private Nimbus.Client _client;
    private static final Logger LOG = LoggerFactory.getLogger(NimbusClient.class);

    public static NimbusClient getConfiguredClient(Map conf) {
        try {
            String nimbusHost = (String) conf.get(Config.NIMBUS_HOST);
            int nimbusPort = Utils.getInt(conf.get(Config.NIMBUS_THRIFT_PORT));
            return new NimbusClient(conf, nimbusHost, nimbusPort);
        } catch (TTransportException ex) {
            throw new RuntimeException(ex);
        }
    }

    public NimbusClient(Map conf, String host, int port) throws TTransportException {
        this(conf, host, port, null);
    }

    public NimbusClient(Map conf, String host, int port, Integer timeout) throws TTransportException {
        super(conf, host, port, timeout);
        _client = new Nimbus.Client(_protocol);
    }

    public Nimbus.Client getClient() {
        return _client;
    }
}
