package backtype.storm.utils;

import backtype.storm.Config;
import backtype.storm.security.auth.ThriftClient;
import backtype.storm.generated.Nimbus;

import java.util.List;
import java.util.Map;
import org.apache.thrift7.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;

public class NimbusClient extends ThriftClient {
    private Nimbus.Client _client;
    private static final Logger LOG = LoggerFactory.getLogger(NimbusClient.class);

    public static NimbusClient getConfiguredClient(Map conf) {
        try {
            String nimbusHost = (String) conf.get(Config.NIMBUS_HOST);
            int nimbusPort = Utils.getInt(conf.get(Config.NIMBUS_THRIFT_PORT));
            if (nimbusPort <= 0) {
                List<String> zk_hosts = (List<String>)conf.get(Config.STORM_ZOOKEEPER_SERVERS);
                Object zk_port = conf.get(Config.STORM_ZOOKEEPER_PORT);
                CuratorFramework zk_fw = Utils.newCurator(conf, 
                        zk_hosts,zk_port,
                        (String) conf.get(Config.STORM_ZOOKEEPER_ROOT), 
                        null); //auth info
                zk_fw.start();
                try {
                    byte[] zk_data = zk_fw.getData().forPath("/nimbus");
                    HostPort nimbus_host_port = (HostPort) Utils.deserialize(zk_data);
                    LOG.debug("Nimbus spec at ZK: "+nimbus_host_port);
                    nimbusHost = nimbus_host_port.host();
                    nimbusPort = nimbus_host_port.port();
                    conf.put(Config.NIMBUS_HOST, nimbusHost);
                    conf.put(Config.NIMBUS_THRIFT_PORT, (Integer)nimbusPort);
                } catch (Exception e) {
                    LOG.warn("Failure in obtaining Nimbus host/port from Zookeeper "+zk_hosts+" port "+zk_port, e);
                } finally {
                    zk_fw.close();
                }
            }
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
